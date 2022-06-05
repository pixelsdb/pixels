package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.C;
import org.openjsse.sun.security.util.Cache;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

// TODO: it can be general as PartitionIndexReader regardless of whether radix or hash
// Protocol is a over-design, dont do this.
public class PartitionCacheReader {
    private static final Logger logger = LogManager.getLogger(PartitionRadixIndexReader.class);
    private final MemoryMappedFile indexWholeRegion;
    private final MemoryMappedFile cacheWholeRegion;
    private final MemoryMappedFile[] cacheSubRegions;
    private final MemoryMappedFile[] indexSubRegions;

    private final int partitions;
    private final long metaSize = PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
    ByteBuffer partitionHashKeyBuf = ByteBuffer.allocate(2 + 2);
    private final CacheIndexReader[] readers; // related to physical partitions, length=partitions+1


    //    public PartitionCacheReader(MemoryMappedFile wholeRegion, int partitions, long subRegionSize, MemoryMappedFile cacheWholeRegion) {
    private PartitionCacheReader(MemoryMappedFile indexWholeRegion, MemoryMappedFile[] indexSubRegions,
                                MemoryMappedFile cacheWholeRegion, MemoryMappedFile[] cacheSubRegions,
                                int partitions) {
        assert (indexSubRegions.length == partitions + 1);
        assert (cacheSubRegions.length == partitions + 1);

        this.cacheWholeRegion = cacheWholeRegion;
        this.indexWholeRegion = indexWholeRegion;
        this.partitions = partitions;
        this.readers = new CacheIndexReader[partitions + 1];
        this.cacheSubRegions = cacheSubRegions;
        this.indexSubRegions = indexSubRegions;
        for (int i = 0; i < partitions + 1; ++i) {
            this.readers[i] = new RadixIndexReader(indexSubRegions[i]);
        }
    }

    // TODO: the verification of whether we can read shall be done where? I think this protocol verifier shall be
    //        decoupled

    // this method is supposed to be used with test
    public PixelsCacheIdx search(PixelsCacheKey key) {
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
            readCount = (v & PixelsCacheUtil.READER_COUNT_MASK) >> PixelsCacheUtil.READER_COUNT_RIGHT_SHIFT_BITS;
            if (readCount >= PixelsCacheUtil.MAX_READER_COUNT) {
                return null;
            }
            lease = System.currentTimeMillis();

        } while (rwflag > 0 || !indexSubRegion.compareAndSwapInt(6, v, v + PixelsCacheUtil.READER_COUNT_INC));
        // the loop will exit if rw_flag=0 and we have increased the rw_count atomically at the same time.

        // now we have the access to the region.
        int version = PixelsCacheUtil.getIndexVersion(indexSubRegion);
        logger.trace("physical partition=" + physicalPartition);
        CacheIndexReader reader = readers[physicalPartition];
        PixelsCacheIdx cacheIdx = reader.read(key);

        // check the lease, version and read_count
        v = indexSubRegion.getIntVolatile(6);
        readCount = (v & PixelsCacheUtil.READER_COUNT_MASK) >>
                PixelsCacheUtil.READER_COUNT_RIGHT_SHIFT_BITS;

        if (readCount == 0 || PixelsCacheUtil.getIndexVersion(indexSubRegion) != version
                || System.currentTimeMillis() - lease > PixelsCacheUtil.CACHE_READ_LEASE_MS) {
            // it is a staggler reader, abort the read
            logger.trace("read aborted");
            return null;
        }

        // end indexRead, the cacheIdx is a valid thing to return, we just tries to decrease the read_count
        // it is possible that read_count will be forced to
        // if reader count is already <= 0, nothing will be done, just return
        while ((v & PixelsCacheUtil.READER_COUNT_MASK) > 0)
        {
            if (indexSubRegion.compareAndSwapInt(6, v, v-PixelsCacheUtil.READER_COUNT_INC))
            {
                // if v is not changed and the reader count is successfully decreased, break.
                break;
            }
            v = indexSubRegion.getIntVolatile(6);
        }

        return cacheIdx;
    }

    // TODO: mmap file shall be closed by the caller, not me?
    // return a direct buffer
    public ByteBuffer get(PixelsCacheKey key) {
        // TODO: 这个方案是有缺陷的! 因为返回的只是一个地址的拷贝, 在 reader 实际 process 的时候, writer 是有可能重写这部分区域的
        // TODO: do the protocol part
        partitionHashKeyBuf.putShort(0, key.rowGroupId);
        partitionHashKeyBuf.putShort(2, key.columnId);
        int logicalPartition = PixelsCacheUtil.hashcode(partitionHashKeyBuf.array()) & 0x7fffffff % partitions;
        int physicalPartition = PixelsCacheUtil.retrievePhysicalPartition(indexWholeRegion, logicalPartition, partitions);
        logger.trace("physical partition=" + physicalPartition);
        CacheIndexReader reader = readers[physicalPartition];
        MemoryMappedFile content = cacheSubRegions[physicalPartition];
        PixelsCacheIdx cacheIdx = reader.read(key);
        if (cacheIdx == null) return null;
        return content.getDirectByteBuffer(cacheIdx.offset, cacheIdx.length);
    }

    public int get(PixelsCacheKey key, byte[] buf, int size) {
        // 这个是安全的
        partitionHashKeyBuf.putShort(0, key.rowGroupId);
        partitionHashKeyBuf.putShort(2, key.columnId);
        int logicalPartition = PixelsCacheUtil.hashcode(partitionHashKeyBuf.array()) & 0x7fffffff % partitions;
        int physicalPartition = PixelsCacheUtil.retrievePhysicalPartition(indexWholeRegion, logicalPartition, partitions);
        logger.trace("physical partition=" + physicalPartition);
        CacheIndexReader reader = readers[physicalPartition];
        MemoryMappedFile content = cacheSubRegions[physicalPartition];
        PixelsCacheIdx cacheIdx = reader.read(key);
        if (cacheIdx == null) return 0;
        content.getBytes(cacheIdx.offset, buf, 0, size);
        return size;

    }


    public static class Builder {
        private MemoryMappedFile indexWholeRegion;
        private MemoryMappedFile cacheWholeRegion;
        private int partitions = Integer.parseInt(ConfigFactory.Instance().getProperty("cache.partitions"));
        // TODO: ensure the subRegionBytes is equal to what computed in the ParititonCacheWriter, it might change!
        private long indexSubRegionBytes;
        private long cacheSubRegionBytes;

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

        public PartitionCacheReader.Builder setPartitions(int partitions) {
//            requireNonNull(indexFile, "index file is null");
            this.partitions = partitions;

            return this;
        }

        public PartitionCacheReader build() {
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
            return new PartitionCacheReader(indexWholeRegion, indexSubRegions, cacheWholeRegion, cacheSubRegions, partitions);
        }
    }

    public static PartitionCacheReader.Builder newBuilder() {
        return new PartitionCacheReader.Builder();
    }
}
