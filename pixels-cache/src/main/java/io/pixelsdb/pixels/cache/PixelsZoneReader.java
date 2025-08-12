/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Pixels zone reader.
 * this file is derived from old version of PixelsCacheReader.java
 * It is not thread safe.
 *
 * @author guodong, hank, alph00
 */
public class PixelsZoneReader implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(PixelsZoneReader.class);

    private final MemoryMappedFile zoneFile;
    private final MemoryMappedFile indexFile;

    /**
     * <p>
     *     A node can have at most 256 children, plus an edge (a segment of
     *     lookup key). Each child is 8 bytes.
     * </p>
     * <p>
     *     In pixels, where will be one PixelsZoneReader on each split. And each
     *     split is supposed to be processed by one single thread. There are
     *     typically 10 - 200 concurrent threads (splits) on a machine. So that it
     *     is not a problem to allocate nodeData in each PixelsZoneReader instance.
     *     By this, we can avoid frequent memory allocation in the search() method.
     * </p>
     * <p>
     *     For the edge, although PixelsRadix can support 1MB edge length, we only
     *     use 12 byte path (PixelsCacheKey). That means no edges can exceeds 12 bytes.
     *     So that 16 bytes is more than enough.
     * </p>
     */
    private final byte[] nodeData = new byte[256 * 8 + 16];
    private final ByteBuffer childrenBuffer = ByteBuffer.wrap(nodeData);

    private final ByteBuffer keyBuffer = ByteBuffer.allocate(PixelsCacheKey.SIZE).order(ByteOrder.BIG_ENDIAN);

//    static
//    {
//        new Thread(cacheLogger).start();
//    }

    private PixelsZoneReader(MemoryMappedFile zoneFile, MemoryMappedFile indexFile)
    {
        this.zoneFile = zoneFile;
        this.indexFile = indexFile;
    }

    public PixelsZoneReader(String zoneLocation, String indexLocation, long zoneSize, long indexSize) throws Exception {
        this.zoneFile = new MemoryMappedFile(zoneLocation, zoneSize);
        this.indexFile = new MemoryMappedFile(indexLocation, indexSize);
    }   

    public MemoryMappedFile getZoneFile() {
        return zoneFile;
    }

    public MemoryMappedFile getIndexFile() {
        return indexFile;
    }

    public static class Builder
    {
        private MemoryMappedFile builderZoneFile;
        private MemoryMappedFile builderIndexFile;

        private Builder()
        {
        }

        public PixelsZoneReader.Builder setZoneFile(MemoryMappedFile zoneFile)
        {
//            requireNonNull(zoneFile, "zone file is null");
            this.builderZoneFile = zoneFile;

            return this;
        }

        public PixelsZoneReader.Builder setIndexFile(MemoryMappedFile indexFile)
        {
//            requireNonNull(indexFile, "index file is null");
            this.builderIndexFile = indexFile;

            return this;
        }

        public PixelsZoneReader build()
        {
            return new PixelsZoneReader(builderZoneFile, builderIndexFile);
        }
    }

    public static PixelsZoneReader.Builder newBuilder()
    {
        return new PixelsZoneReader.Builder();
    }

    public ByteBuffer get(long blockId, short rowGroupId, short columnId)
    {
        return this.get(blockId, rowGroupId, columnId, true);
    }

    /**
     * Read specified column chunk from zone.
     * If zone is not hit, empty byte array is returned, and an access message is sent to the mq.
     * If zone is hit, the column chunk content is returned as byte array.
     * This method may return NULL value. Be careful dealing with null!!!
     *
     * @param blockId    block id
     * @param rowGroupId row group id
     * @param columnId   column id
     * @param direct get direct byte buffer if true
     * @return the column chunk content, or null if failed to read the zone
     */
    public ByteBuffer get(long blockId, short rowGroupId, short columnId, boolean direct)
    {
        // search index file for column chunk id
        PixelsCacheKey.getBytes(keyBuffer, blockId, rowGroupId, columnId);

        // check the rwFlag and increase readCount.
        long lease = 0;
        try
        {
            lease = PixelsZoneUtil.beginIndexRead(indexFile);
        }
        catch (InterruptedException e)
        {
            logger.error("Failed to get read permission on index.", e);
            /**
             * Issue #88:
             * In case of failure (e.g. reaches max zone reader count),
             * return null here to stop reading zone, then the content
             * will be read from disk.
             */
            return null;
        }

        ByteBuffer content = null;
        // search zone key
//        long searchBegin = System.nanoTime();
        PixelsCacheIdx zoneIdx = search(keyBuffer);
//        long searchEnd = System.nanoTime();
//        cacheLogger.addSearchLatency(searchEnd - searchBegin);
//        logger.debug("[zone search]: " + (searchEnd - searchBegin));
        // if found, read content from zone
//        long readBegin = System.nanoTime();
        if (zoneIdx != null)
        {
            if (direct)
            {
                // read content
                content = zoneFile.getDirectByteBuffer(zoneIdx.offset, zoneIdx.length);
            }
            else
            {
                content = ByteBuffer.allocate(zoneIdx.length);
                // read content
                zoneFile.getBytes(zoneIdx.offset, content.array(), 0, zoneIdx.length);
            }
        }

        boolean cacheReadSuccess = PixelsZoneUtil.endIndexRead(indexFile, lease);

//        long readEnd = System.nanoTime();
//        cacheLogger.addReadLatency(readEnd - readBegin);
//        logger.debug("[zone read]: " + (readEnd - readBegin));
        if (cacheReadSuccess)
        {
            return content;
        }
        return null;
    }

    public void batchGet(List<ColumnChunkId> columnChunkIds, byte[][] container)
    {
        // TODO batch get zone items. merge zone accesses to reduce the number of jni invocation.
    }

    /**
     * This interface is only used by TESTS, DO NOT USE.
     * It will be removed soon!
     */
    public PixelsCacheIdx search(long blockId, short rowGroupId, short columnId)
    {
        PixelsCacheKey.getBytes(keyBuffer, blockId, rowGroupId, columnId);

        return search(keyBuffer);
    }

    /**
     * Search key from radix tree.
     * If found, update counter in zone idx.
     * Else, return null
     */
    private PixelsCacheIdx search(ByteBuffer keyBuffer)
    {
        int dramAccessCounter = 0;
        int radixLevel = 0;
        final int keyLen = keyBuffer.position();
        long currentNodeOffset = PixelsZoneUtil.INDEX_RADIX_OFFSET;
        int bytesMatched = 0;
        int bytesMatchedInNodeFound = 0;

        // get root
        // TODO: root currently does not have edge, which is not efficient in some cases.
        int currentNodeHeader = indexFile.getInt(currentNodeOffset);
        dramAccessCounter++;
        int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
        int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
        if (currentNodeChildrenNum == 0 && currentNodeEdgeSize == 0)
        {
            return null;
        }
        indexFile.getBytes(currentNodeOffset + 4, this.nodeData, 0, currentNodeChildrenNum * 8);
        dramAccessCounter++;
        radixLevel++;

        // search
        // it can be more clear with a do while function
        outer_loop:
        while (bytesMatched < keyLen)
        {
            // search each child for the matching node
            long matchingChildOffset = 0L;
            childrenBuffer.position(0);
            childrenBuffer.limit(currentNodeChildrenNum * 8);
            // linearly scan all the children
            for (int i = 0; i < currentNodeChildrenNum; i++)
            {
                // long is 8 byte, which is 64 bit
                long child = childrenBuffer.getLong();
                // first byte is matching byte
                byte leader = (byte) ((child >>> 56) & 0xFF);
                if (leader == keyBuffer.get(bytesMatched))
                {
                    // child last 7 bytes is grandson's offset, that is, the address is 7-bytes long
                    matchingChildOffset = child & 0x00FFFFFFFFFFFFFFL;
                    break;
                }
            }
            if (matchingChildOffset == 0)
            {
                break;
            }

            currentNodeOffset = matchingChildOffset;
            bytesMatchedInNodeFound = 0;
            currentNodeHeader = indexFile.getInt(currentNodeOffset);
            dramAccessCounter++;
            currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
            currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
            // read the children and edge in one memory access.
            indexFile.getBytes(currentNodeOffset + 4,
                    this.nodeData, 0, currentNodeChildrenNum * 8 + currentNodeEdgeSize);
            dramAccessCounter++;
            int edgeEndOffset = currentNodeChildrenNum * 8 + currentNodeEdgeSize;
            /**
             * The first byte is matched in the child leader of the parent node,
             * therefore we start the matching from the second byte in edge.
             * this is useful for below `bytesMatchedInNodeFound == currentNodeEdgeSize`
             */
            bytesMatched++;
            bytesMatchedInNodeFound++;
            // now we are visiting the edge! rather than children data
            // it seems between children and edge, there is a one byte gap?
            // or the first byte of the edge does not matter here anyway
            for (int i = currentNodeChildrenNum * 8 + 1; i < edgeEndOffset && bytesMatched < keyLen; i++)
            {
                // the edge is shared across this node, so the edge should be fully matched
                if (this.nodeData[i] != keyBuffer.get(bytesMatched))
                {
                    break outer_loop;
                }
                bytesMatched++;
                bytesMatchedInNodeFound++;
            }

            // only increase level when a child is really matched.
            radixLevel++;
        }

        // if matches, node found.
        if (bytesMatched == keyLen && bytesMatchedInNodeFound == currentNodeEdgeSize)
        {
            // if the current node is leaf node.
            if (((currentNodeHeader >>> 31) & 1) > 0)
            {
                byte[] idx = new byte[12];
                indexFile.getBytes(currentNodeOffset + 4 + (currentNodeChildrenNum * 8) + currentNodeEdgeSize,
                        idx, 0, 12);
                dramAccessCounter++;
                PixelsCacheIdx zoneIdx = new PixelsCacheIdx(idx);
                zoneIdx.dramAccessCount = dramAccessCounter;
                zoneIdx.radixLevel = radixLevel;
                return zoneIdx;
            }
        }
        return null;
    }

    public void close()
    {
        try
        {
            if (zoneFile != null) 
            {
                zoneFile.unmap();
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to unmap zone file", e);
        }
        try
        {
            if (indexFile != null) 
            {
                indexFile.unmap();
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to unmap index file", e);
        }
    }
}
