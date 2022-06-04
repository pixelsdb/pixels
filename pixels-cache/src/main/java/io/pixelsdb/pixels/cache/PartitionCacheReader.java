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
    private final int partitions;
    private final long metaSize = PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
    ByteBuffer partitionHashKeyBuf = ByteBuffer.allocate(2 + 2);
    private final CacheIndexReader[] readers; // related to physical partitions


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
        int physicalPartition = PixelsCacheUtil.retrievePhysicalPartition(indexWholeRegion, logicalPartition, partitions);
        logger.trace("physical partition=" + physicalPartition);
        CacheIndexReader reader = readers[physicalPartition];
        // TODO: the protocol part, now we can test the correctness without the protocol
        return reader.read(key);
    }
    // TODO: mmap file shall be closed by the caller, not me?
    // return a direct buffer
    public ByteBuffer get(PixelsCacheKey key) {
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
