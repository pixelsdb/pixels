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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * pixels cache reader.
 *
 * @author guodong
 * @author hank
 */
public class PixelsCacheReader
        implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(PixelsCacheReader.class);
    // private static CacheLogger cacheLogger = new CacheLogger();

    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;

    /**
     * <p>
     *     A node can have at most 256 children, plus an edge (a segment of
     *     lookup key). Each child is 8 bytes.
     * </p>
     * <p>
     *     In pixels, where will be one PixelsCacheReader on each split. And each
     *     split is supposed to be processed by one single thread. There are
     *     typically 10 - 200 concurrent threads (splits) on a machine. So that it
     *     is not a problem to allocate nodeData in each PixelsCacheReader instance.
     *     By this, we can avoid frequent memory allocation in the search() method.
     * </p>
     * <p>
     *     For the edge, although PixelsRadix can support 1MB edge length, we only
     *     use 12 byte path (PixelsCacheKey). That means no edges can exceeds 12 bytes.
     *     So that 16 bytes is more than enough.
     * </p>
     */
    private byte[] nodeData = new byte[256 * 8 + 16];
    private ByteBuffer childrenBuffer = ByteBuffer.wrap(nodeData);

    private ByteBuffer keyBuffer = ByteBuffer.allocate(PixelsCacheKey.SIZE).order(ByteOrder.BIG_ENDIAN);

//    static
//    {
//        new Thread(cacheLogger).start();
//    }

    private PixelsCacheReader(MemoryMappedFile cacheFile, MemoryMappedFile indexFile)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
    }

    public static class Builder
    {
        private MemoryMappedFile builderCacheFile;
        private MemoryMappedFile builderIndexFile;

        private Builder()
        {
        }

        public PixelsCacheReader.Builder setCacheFile(MemoryMappedFile cacheFile)
        {
//            requireNonNull(cacheFile, "cache file is null");
            this.builderCacheFile = cacheFile;

            return this;
        }

        public PixelsCacheReader.Builder setIndexFile(MemoryMappedFile indexFile)
        {
//            requireNonNull(indexFile, "index file is null");
            this.builderIndexFile = indexFile;

            return this;
        }

        public PixelsCacheReader build()
        {
            return new PixelsCacheReader(builderCacheFile, builderIndexFile);
        }
    }

    public static PixelsCacheReader.Builder newBuilder()
    {
        return new PixelsCacheReader.Builder();
    }

    public ByteBuffer get(long blockId, short rowGroupId, short columnId)
    {
        return this.get(blockId, rowGroupId, columnId, true);
    }

    /**
     * Read specified columnlet from cache.
     * If cache is not hit, empty byte array is returned, and an access message is sent to the mq.
     * If cache is hit, columnlet content is returned as byte array.
     * This method may return NULL value. Be careful dealing with null!!!
     *
     * @param blockId    block id
     * @param rowGroupId row group id
     * @param columnId   column id
     * @param direct get direct byte buffer if true
     * @return columnlet content
     */
    public ByteBuffer get(long blockId, short rowGroupId, short columnId, boolean direct)
    {
        // search index file for columnlet id
        PixelsCacheKeyUtil.getBytes(keyBuffer, blockId, rowGroupId, columnId);

        // check the rwFlag and increase readCount.
        try
        {
            PixelsCacheUtil.beginIndexRead(indexFile);
        } catch (InterruptedException e)
        {
            logger.error("Failed to get read permission on index.", e);
        }

        ByteBuffer content = null;
        // search cache key
//        long searchBegin = System.nanoTime();
        PixelsCacheIdx cacheIdx = search(keyBuffer);
//        long searchEnd = System.nanoTime();
//        cacheLogger.addSearchLatency(searchEnd - searchBegin);
//        logger.debug("[cache search]: " + (searchEnd - searchBegin));
        // if found, read content from cache
//        long readBegin = System.nanoTime();
        if (cacheIdx != null)
        {
            if (direct)
            {
                // read content
                content = cacheFile.getDirectByteBuffer(cacheIdx.offset, cacheIdx.length);
            }
            else
            {
                content = ByteBuffer.allocate(cacheIdx.length);
                // read content
                cacheFile.getBytes(cacheIdx.offset, content.array(), 0, cacheIdx.length);
            }
        }

        PixelsCacheUtil.endIndexRead(indexFile);

//        long readEnd = System.nanoTime();
//        cacheLogger.addReadLatency(readEnd - readBegin);
//        logger.debug("[cache read]: " + (readEnd - readBegin));
        return content;
    }

    public void batchGet(List<ColumnletId> columnletIds, byte[][] container)
    {
        // TODO batch get cache items. merge cache accesses to reduce the number of jni invocation.
    }

    /**
     * This interface is only used by TESTS, DO NOT USE.
     * It will be removed soon!
     */
    public PixelsCacheIdx search(long blockId, short rowGroupId, short columnId)
    {
        PixelsCacheKeyUtil.getBytes(keyBuffer, blockId, rowGroupId, columnId);

        return search(keyBuffer);
    }

    /**
     * Search key from radix tree.
     * If found, update counter in cache idx.
     * Else, return null
     */
    private PixelsCacheIdx search(ByteBuffer keyBuffer)
    {
        int dramAccessCounter = 0;
        int radixLevel = 0;
        final int keyLen = keyBuffer.position();
        long currentNodeOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
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
        outer_loop:
        while (bytesMatched < keyLen)
        {
            // search each child for the matching node
            long matchingChildOffset = 0L;
            childrenBuffer.position(0);
            childrenBuffer.limit(currentNodeChildrenNum * 8);
            for (int i = 0; i < currentNodeChildrenNum; i++)
            {
                long child = childrenBuffer.getLong();
                byte leader = (byte) ((child >>> 56) & 0xFF);
                if (leader == keyBuffer.get(bytesMatched))
                {
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
             */
            bytesMatched++;
            bytesMatchedInNodeFound++;
            for (int i = currentNodeChildrenNum * 8 + 1; i < edgeEndOffset && bytesMatched < keyLen; i++)
            {
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
                PixelsCacheIdx cacheIdx = new PixelsCacheIdx(idx);
                cacheIdx.dramAccessCount = dramAccessCounter;
                cacheIdx.radixLevel = radixLevel;
                return cacheIdx;
            }
        }
        return null;
    }

    public void close()
    {
        try
        {
//            logger.info("cache reader unmaps cache/index file");
            cacheFile.unmap();
            indexFile.unmap();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
