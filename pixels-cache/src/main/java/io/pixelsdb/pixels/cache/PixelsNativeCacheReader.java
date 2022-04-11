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
 * It is not thread safe.
 *
 * @author yeeef
 */
public class PixelsNativeCacheReader
        implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(PixelsNativeCacheReader.class);
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

    private PixelsNativeCacheReader(MemoryMappedFile cacheFile, MemoryMappedFile indexFile)
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

        public PixelsNativeCacheReader.Builder setCacheFile(MemoryMappedFile cacheFile)
        {
//            requireNonNull(cacheFile, "cache file is null");
            this.builderCacheFile = cacheFile;

            return this;
        }

        public PixelsNativeCacheReader.Builder setIndexFile(MemoryMappedFile indexFile)
        {
//            requireNonNull(indexFile, "index file is null");
            this.builderIndexFile = indexFile;

            return this;
        }

        public PixelsNativeCacheReader build()
        {
            return new PixelsNativeCacheReader(builderCacheFile, builderIndexFile);
        }
    }

    public static PixelsNativeCacheReader.Builder newBuilder()
    {
        return new PixelsNativeCacheReader.Builder();
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
     * @return columnlet content, null if failed to read cache.
     */
    public ByteBuffer get(long blockId, short rowGroupId, short columnId, boolean direct)
    {
        // search index file for columnlet id
        PixelsCacheKey.getBytes(keyBuffer, blockId, rowGroupId, columnId);

        // check the rwFlag and increase readCount.
        long lease = 0;
        try
        {
            lease = PixelsCacheUtil.beginIndexRead(indexFile);
        } catch (InterruptedException e)
        {
            logger.error("Failed to get read permission on index.", e);
            /**
             * Issue #88:
             * In case of failure (e.g. reaches max cache reader count),
             * return null here to stop reading cache, then the content
             * will be read from disk.
             */
            return null;
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

        boolean cacheReadSuccess = PixelsCacheUtil.endIndexRead(indexFile, lease);

//        long readEnd = System.nanoTime();
//        cacheLogger.addReadLatency(readEnd - readBegin);
//        logger.debug("[cache read]: " + (readEnd - readBegin));
        if (cacheReadSuccess)
        {
            return content;
        }
        return null;
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
        PixelsCacheKey.getBytes(keyBuffer, blockId, rowGroupId, columnId);

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
        // store the metadata like rw_flag and reader_cnt
        long currentNodeOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
        int bytesMatched = 0;
        int bytesMatchedInNodeFound = 0;

        // get root
        // TODO: root currently does not have edge, which is not efficient in some cases.
        // header contains children num and edge size
        int currentNodeHeader = indexFile.getInt(currentNodeOffset);
        dramAccessCounter++;
        // currentNodeHeader is 32 bit, 4bytes on a 64-bit machine
        // last 4+4+1=9 bits encode the number of the children
        int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
        // 7FFFFE means 22 bits encode the edge size (the first bit is reserved)
        int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
        if (currentNodeChildrenNum == 0 && currentNodeEdgeSize == 0) // cache is empty
        {
            return null;
        }
        // read all childrent data in this.nodeData, each child node has 8 bytes
        // the header is 4 bytes, so the pos is currentNodeOffset + 4
        indexFile.getBytes(currentNodeOffset + 4, this.nodeData, 0, currentNodeChildrenNum * 8);
        dramAccessCounter++;
        radixLevel++;
        // the span of our radix tree is 8 bits(1 byte), so we can have at most 2^8=256 children
        // the radix tree is on byte level

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
                // long has 8 bytes, which is a child's bytes
                long child = childrenBuffer.getLong();
                // the first byte is the leader of the child
                byte leader = (byte) ((child >>> 56) & 0xFF); // TODO: why?
                // ok, 又略懂一些了, 这个 radix tree 是以 byte 为单位的(而不是 char),
                // 我们希望查找的 "string" 就是 keyBuffer 这个 []byte.
                if (leader == keyBuffer.get(bytesMatched))
                {
                    // match a byte, then we can go to next level(byte)
                    // last 7 bytes is the offset(pointer to the children position)
                    matchingChildOffset = child & 0x00FFFFFFFFFFFFFFL; // TODO: hit?
                    break;
                }
            }
            if (matchingChildOffset == 0) // cache miss
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
            // Note: now this nodeData also contains the edge!
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
            // traverse the edge, i is the byte. TODO: why start at num*8 + 1, where does +1 comes from?
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

    private native byte[] search(long mmAddress, long mmSize, long blockId, short rowGroupId, short columnId);

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
