/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.cache.legacy;

import io.pixelsdb.pixels.cache.PixelsCacheIdx;
import io.pixelsdb.pixels.cache.PixelsCacheKey;
import io.pixelsdb.pixels.cache.PixelsCacheUtil;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;

// TODO: it can be general as PartitionIndexReader regardless of whether radix or hash
// Protocol is a over-design, dont do this.
public class PartitionRadixIndexReader implements CacheIndexReader {
    private static final Logger logger = LogManager.getLogger(PartitionRadixIndexReader.class);
    private final MemoryMappedFile wholeRegion;
    private final int partitions;
    private final long subRegionSize;
    private final long metaSize = PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
    ByteBuffer partitionHashKeyBuf = ByteBuffer.allocate(2 + 2);
    private final CacheIndexReader[] readers; // related to physical partitions


    public PartitionRadixIndexReader(MemoryMappedFile wholeRegion, int partitions, long subRegionSize) {
        this.wholeRegion = wholeRegion;
        this.partitions = partitions;
        this.subRegionSize = subRegionSize;
        this.readers = new CacheIndexReader[partitions + 1];
        for (int i = 0; i < partitions + 1; ++i) {
            this.readers[i] = new RadixIndexReader(this.wholeRegion.regionView(metaSize + i * subRegionSize, subRegionSize));
        }
    }

    // TODO: the verification of whether we can read shall be done where? I think this protocol verifier shall be
    //        decoupled
    @Override
    public PixelsCacheIdx read(PixelsCacheKey key) {
        // retrieve freePhysical + startPhysical
        partitionHashKeyBuf.putShort(0, key.rowGroupId);
        partitionHashKeyBuf.putShort(2, key.columnId);
        int logicalPartition = PixelsCacheUtil.hashcode(partitionHashKeyBuf.array()) & 0x7fffffff % partitions;
        int physicalPartition = PixelsCacheUtil.retrievePhysicalPartition(wholeRegion, logicalPartition, partitions);
        logger.trace("physical partition=" + physicalPartition);
        CacheIndexReader reader = readers[physicalPartition];
        // TODO: the protocol part, now we can test the correctness without the protocol
        PixelsCacheIdx cacheIdx = reader.read(key);
        if (cacheIdx == null) return null;
        return new PixelsCacheIdx(cacheIdx.offset, cacheIdx.length, physicalPartition);
    }
    // TODO: mmap file shall be closed by the caller, not me.

    public static class Builder
    {
        private MemoryMappedFile wholeRegion;
        private int partitions = Integer.parseInt(ConfigFactory.Instance().getProperty("cache.partitions"));
        private long subRegionBytes = Long.parseLong(ConfigFactory.Instance().getProperty("index.size")) / partitions;

        public Builder()
        {
            subRegionBytes = Long.parseLong(ConfigFactory.Instance().getProperty("index.size")) / partitions;
        }


        public PartitionRadixIndexReader.Builder setIndexFile(MemoryMappedFile indexFile)
        {
//            requireNonNull(indexFile, "index file is null");
            this.wholeRegion = indexFile;

            return this;
        }

        public PartitionRadixIndexReader.Builder setPartitions(int partitions)
        {
//            requireNonNull(indexFile, "index file is null");
            this.partitions = partitions;

            return this;
        }

        public PartitionRadixIndexReader.Builder setSubRegionBytes(long size) {
            this.subRegionBytes = size;
            return this;
        }

        public PartitionRadixIndexReader build()
        {
            return new PartitionRadixIndexReader(wholeRegion, partitions, subRegionBytes);
        }
    }

    public static PartitionRadixIndexReader.Builder newBuilder()
    {
        return new PartitionRadixIndexReader.Builder();
    }
}
