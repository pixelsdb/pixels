package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.function.Supplier;

// TODO: it can be general as PartitionIndexReader regardless of whether radix or hash
// Protocol is a over-design, dont do this.
public class PartitionCacheReader implements CacheReader {
    private static final Logger logger = LogManager.getLogger(PartitionRadixIndexReader.class);
    private final MemoryMappedFile indexWholeRegion;
    private final MemoryMappedFile cacheWholeRegion;
    private final MemoryMappedFile[] cacheSubRegions;
    private final MemoryMappedFile[] indexSubRegions;

    private final int partitions;
    private final long metaSize = PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
    ByteBuffer partitionHashKeyBuf = ByteBuffer.allocate(2 + 2);
    // Note: we cannot directly use the PartitionedRadixIndexReader, as the read-write protocol is tightly coupled
    private final CacheIndexReader[] readers; // related to physical partitions, length=partitions+1


    private PartitionCacheReader(MemoryMappedFile indexWholeRegion, MemoryMappedFile[] indexSubRegions,
                                MemoryMappedFile cacheWholeRegion, MemoryMappedFile[] cacheSubRegions,
                                CacheIndexReader[] readers,
                                int partitions) {
        assert (indexSubRegions.length == partitions + 1);
        assert (cacheSubRegions.length == partitions + 1);
        assert (readers.length == partitions + 1);

        this.cacheWholeRegion = cacheWholeRegion;
        this.indexWholeRegion = indexWholeRegion;
        this.partitions = partitions;
        this.cacheSubRegions = cacheSubRegions;
        this.indexSubRegions = indexSubRegions;
        this.readers = readers;
    }

    // TODO: the verification of whether we can read shall be done where? I think this protocol verifier shall be
    //        decoupled

    public PixelsCacheIdx naiveSearch(PixelsCacheKey key) {
        partitionHashKeyBuf.putShort(0, key.rowGroupId);
        partitionHashKeyBuf.putShort(2, key.columnId);
        int logicalPartition = PixelsCacheUtil.hashcode(partitionHashKeyBuf.array()) & 0x7fffffff % partitions;
        int physicalPartition = PixelsCacheUtil.retrievePhysicalPartition(indexWholeRegion, logicalPartition, partitions);
        logger.trace("physical partition=" + physicalPartition);
        CacheIndexReader reader = readers[physicalPartition];
        return reader.read(key);
    }

    public PixelsCacheIdx simpleSearch(PixelsCacheKey key) {
        // retrieve freePhysical + startPhysical
        partitionHashKeyBuf.putShort(0, key.rowGroupId);
        partitionHashKeyBuf.putShort(2, key.columnId);
        int logicalPartition = PixelsCacheUtil.hashcode(partitionHashKeyBuf.array()) & 0x7fffffff % partitions;
        int physicalPartition;
        MemoryMappedFile indexSubRegion;
        int rwflag = 0;
        int readCount = 0;
        int v;
        long lease;
        do {
            // 0. use free and start to decide the physical partition
            physicalPartition = PixelsCacheUtil.retrievePhysicalPartition(indexWholeRegion,
                    logicalPartition, partitions);
            indexSubRegion = indexSubRegions[physicalPartition];
            // 1. check rw_flag; it is possible that before writer change the free and start, the reader comes in
            // and routed to the next free physical partition.
            v = indexSubRegion.getIntVolatile(6);
            rwflag = v & PixelsCacheUtil.RW_MASK;
            lease = System.currentTimeMillis();

        } while (rwflag > 0);
        // the loop will exit if rw_flag=0 and we have increased the rw_count atomically at the same time.

        // now we have the access to the region.
        logger.trace("physical partition=" + physicalPartition);
        CacheIndexReader reader = readers[physicalPartition];
        PixelsCacheIdx cacheIdx = reader.read(key);

        // check the lease, version and read_count
        if (System.currentTimeMillis() - lease > PixelsCacheUtil.CACHE_READ_LEASE_MS * 2) {
            // it is a staggler reader, abort the read
            logger.debug("read aborted elapsed=" + (System.currentTimeMillis() - lease) + " partition=" + logicalPartition);
            return null;
        }

        // end indexRead, the cacheIdx is a valid thing to return, we just tries to decrease the read_count
        // it is possible that read_count will be forced to
        // if reader count is already <= 0, nothing will be done, just return
        return cacheIdx;
    }

    static class ReadLease {
        long startMilis;
        int physicalPartition;
        int version;
        boolean valid;
        long expireMilis = PixelsCacheUtil.CACHE_READ_LEASE_MS;
        ReadLease(boolean valid) { this.valid = valid; }
        ReadLease(long start, int partition, int version) {
            startMilis = start;
            physicalPartition = partition;
            valid = true;
            this.version = version;
        }
        static ReadLease invalid() {
            return new ReadLease(false);
        }
        boolean isValid(int currentVersion) {
            return valid && version == currentVersion && System.currentTimeMillis() - startMilis <= expireMilis;
        }
    }

    private ReadLease prepareRead(PixelsCacheKey key) {
        partitionHashKeyBuf.putShort(0, key.rowGroupId);
        partitionHashKeyBuf.putShort(2, key.columnId);
        int logicalPartition = PixelsCacheUtil.hashcode(partitionHashKeyBuf.array()) & 0x7fffffff % partitions;
        int physicalPartition;
        MemoryMappedFile indexSubRegion;
        int rwflag = 0;
        int readCount = 0;
        int v;
        long start;
        do {
            // 0. use free and start to decide the physical partition
            physicalPartition = PixelsCacheUtil.retrievePhysicalPartition(indexWholeRegion,
                    logicalPartition, partitions);
            indexSubRegion = indexSubRegions[physicalPartition];
            // 1. check rw_flag; it is possible that before writer change the free and start, the reader comes in
            // and routed to the next free physical partition.
            v = indexSubRegion.getIntVolatile(6);
            rwflag = v & PixelsCacheUtil.RW_MASK;
            readCount = (v & PixelsCacheUtil.READER_COUNT_MASK) >> PixelsCacheUtil.READER_COUNT_RIGHT_SHIFT_BITS;
            if (readCount >= PixelsCacheUtil.MAX_READER_COUNT) {
                return ReadLease.invalid();
            }
            start = System.currentTimeMillis();

        } while (rwflag > 0 || !indexSubRegion.compareAndSwapInt(6, v, v + PixelsCacheUtil.READER_COUNT_INC));

        return new ReadLease(start, physicalPartition, PixelsCacheUtil.getIndexVersion(indexSubRegion));
    }

    private boolean endRead(ReadLease lease) {
        MemoryMappedFile indexSubRegion = indexSubRegions[lease.physicalPartition];
        // check the lease, version and read_count
        int v = indexSubRegion.getIntVolatile(6);
        int readCount = (v & PixelsCacheUtil.READER_COUNT_MASK) >>
                PixelsCacheUtil.READER_COUNT_RIGHT_SHIFT_BITS;

        if (readCount == 0 || !lease.isValid(PixelsCacheUtil.getIndexVersion(indexSubRegion))) {
            logger.debug("read aborted readCount=" + readCount + " " + (System.currentTimeMillis() - lease.startMilis));
            return false;
        }
        while ((v & PixelsCacheUtil.READER_COUNT_MASK) > 0)
        {
            if (indexSubRegion.compareAndSwapInt(6, v, v-PixelsCacheUtil.READER_COUNT_INC))
            {
                // if v is not changed and the reader count is successfully decreased, break.
                break;
            }
            v = indexSubRegion.getIntVolatile(6);
        }
        return true;
    }

    // this method is supposed to be used with test
    public PixelsCacheIdx search(PixelsCacheKey key) {
        ReadLease lease = prepareRead(key);

        logger.trace("physical partition=" + lease.physicalPartition);
        CacheIndexReader reader = readers[lease.physicalPartition];
        PixelsCacheIdx cacheIdx = reader.read(key);

        if (endRead(lease) && cacheIdx != null)
            return new PixelsCacheIdx(cacheIdx.offset, cacheIdx.length, lease.physicalPartition);
        else
            return null;
    }

    // TODO: mmap file shall be closed by the caller, not me?
    // return a direct buffer
    public ByteBuffer get(PixelsCacheKey key) {
        // TODO: 这个方案是有缺陷的! 因为返回的只是一个地址的拷贝, 在 reader 实际 process 的时候, writer 是有可能重写这部分区域的
        ReadLease lease = prepareRead(key);

        logger.trace("physical partition=" + lease.physicalPartition);
        CacheIndexReader reader = readers[lease.physicalPartition];
        MemoryMappedFile content = cacheSubRegions[lease.physicalPartition];

        PixelsCacheIdx cacheIdx = reader.read(key);

        if (endRead(lease) && cacheIdx != null) {
            byte[] buf = new byte[cacheIdx.length];
            content.getBytes(cacheIdx.offset, buf, 0, cacheIdx.length);
            ByteBuffer ret = ByteBuffer.wrap(buf);
            return ret;
//            return content.getDirectByteBuffer(cacheIdx.offset, cacheIdx.length);
        }
        else
            return null;
    }

    // TODO: what if buf.length is not enough to hold?
    public int get(PixelsCacheKey key, byte[] buf, int size) {
        ReadLease lease = prepareRead(key);
        logger.trace("physical partition=" + lease.physicalPartition);
        CacheIndexReader reader = readers[lease.physicalPartition];
        MemoryMappedFile content = cacheSubRegions[lease.physicalPartition];

        PixelsCacheIdx cacheIdx = reader.read(key);

        if (endRead(lease) && cacheIdx != null) {
            content.getBytes(cacheIdx.offset, buf, 0, size);
            return size;
        }
        else
            return 0;
    }


    public static class Builder {
        private MemoryMappedFile indexWholeRegion;
        private MemoryMappedFile cacheWholeRegion;
        private int partitions = Integer.parseInt(ConfigFactory.Instance().getProperty("cache.partitions"));
        // TODO: ensure the subRegionBytes is equal to what computed in the ParititonCacheWriter, it might change!
        private long indexSubRegionBytes;
        private long cacheSubRegionBytes;
        private Function<MemoryMappedFile, CacheIndexReader> indexReaderFactory = RadixIndexReader::new;

        private Builder() {
        }


        public PartitionCacheReader.Builder setIndexFile(MemoryMappedFile indexFile) {
//            requireNonNull(indexFile, "index file is null");
            this.indexWholeRegion = indexFile;

            return this;
        }

        public PartitionCacheReader.Builder setCacheFile(MemoryMappedFile cacheFile) {
//            requireNonNull(indexFile, "index file is null");
            this.cacheWholeRegion = cacheFile;

            return this;
        }

        public PartitionCacheReader.Builder setCacheIndexReader(Function<MemoryMappedFile, CacheIndexReader> factory) {
//            requireNonNull(indexFile, "index file is null");
            indexReaderFactory = factory;

            return this;
        }

        public PartitionCacheReader.Builder setPartitions(int partitions) {
//            requireNonNull(indexFile, "index file is null");
            this.partitions = partitions;

            return this;
        }

        public PartitionCacheReader build() {
            assert(indexReaderFactory != null);
            indexSubRegionBytes = Long.parseLong(ConfigFactory.Instance().getProperty("index.size")) / partitions;
            cacheSubRegionBytes = Long.parseLong(ConfigFactory.Instance().getProperty("cache.size")) / partitions;
            MemoryMappedFile[] indexSubRegions = new MemoryMappedFile[partitions + 1];
            MemoryMappedFile[] cacheSubRegions = new MemoryMappedFile[partitions + 1];
            for (int i = 0; i < partitions + 1; ++i) {
                indexSubRegions[i] = indexWholeRegion.regionView(
                        PixelsCacheUtil.PARTITION_INDEX_META_SIZE + i * indexSubRegionBytes, indexSubRegionBytes);
                cacheSubRegions[i] = cacheWholeRegion.regionView(
                        PixelsCacheUtil.CACHE_DATA_OFFSET + i * cacheSubRegionBytes, cacheSubRegionBytes);
            }
            CacheIndexReader[] indexReaders = new CacheIndexReader[partitions + 1];
            for (int i = 0; i < partitions + 1; ++i) {
                indexReaders[i] = this.indexReaderFactory.apply(indexSubRegions[i]);
            }
            return new PartitionCacheReader(indexWholeRegion, indexSubRegions, cacheWholeRegion, cacheSubRegions,
                    indexReaders, partitions);
        }
    }

    public static PartitionCacheReader.Builder newBuilder() {
        return new PartitionCacheReader.Builder();
    }
}
