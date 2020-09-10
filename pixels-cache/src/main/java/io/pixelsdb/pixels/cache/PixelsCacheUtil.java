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

import io.pixelsdb.pixels.common.exception.CacheException;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.directory.api.util.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * pixels cache header
 * index:
 * - HEADER: MAGIC(6 bytes), RW(2 bytes), READER_COUNT(2 bytes), VERSION(4 bytes)
 * - RADIX
 * cache:
 * - HEADER: MAGIC(6 bytes), STATUS(2 bytes), SIZE(8 bytes)
 * - CONTENT
 *
 * @author guodong
 * @author hank
 */
public class PixelsCacheUtil
{
    private final static Logger logger = LogManager.getLogger(PixelsCacheUtil.class);

    public static final int MAX_READER_COUNT = 2 ^ 15 - 1;

    public static final int RW_MASK;
    public static final int READER_COUNT_MASK;
    public static final int READER_COUNT_INC;
    /**
     * We only use the first 14 bytes in the index {magic(6)+rwflag(2)+readcount(2)+version(4)}
     * for metadata header, but we start radix tree from offset 16 for memory alignment.
     */
    public static final int INDEX_RADIX_OFFSET = 16;
    /**
     * We use the first 16 bytes in the cache file {magic(6)+status(2)+size(8)} for
     * metadata header.
     */
    public static final int CACHE_DATA_OFFSET = 16;

    static
    {
        if (MemoryMappedFile.getOrder() == ByteOrder.LITTLE_ENDIAN)
        {
            /**
             * If the index file is in little-endian, rw flag is in the lowest
             * two bytes of v, while reader count is in the highest two bytes.
             */
            RW_MASK = 0x0000ffff;
            READER_COUNT_MASK = 0xffff0000;
            READER_COUNT_INC = 0x00010000;
        }
        else
        {
            /**
             * If the index file is in big-endian, rw flag is in the highest
             * two bytes of v, while reader count is in the lowest two bytes.
             */
            RW_MASK = 0xffff0000;
            READER_COUNT_MASK = 0x0000ffff;
            READER_COUNT_INC = 0x00000001;
        }
    }

    public enum CacheStatus
    {
        INCONSISTENT((short) -1), EMPTY((short) 0), OK((short) 1);

        private final short id;

        CacheStatus(short id)
        {
            this.id = id;
        }

        public short getId()
        {
            return id;
        }
    }

    public static void initialize(MemoryMappedFile indexFile, MemoryMappedFile cacheFile)
    {
        // init index
        setMagic(indexFile);
        clearIndexRWAndCount(indexFile);
        setIndexVersion(indexFile, 0);
        // init cache
        setMagic(cacheFile);
        setCacheStatus(cacheFile, CacheStatus.EMPTY.getId());
        setCacheSize(cacheFile, 0);
    }

    private static void setMagic(MemoryMappedFile file)
    {
        file.putBytes(0, Constants.MAGIC.getBytes(StandardCharsets.UTF_8));
    }

    public static String getMagic(MemoryMappedFile file)
    {
        byte[] magic = new byte[6];
        file.getBytes(0, magic, 0, 6);
        return Strings.getString(magic, StandardCharsets.UTF_8.displayName());
    }

    public static boolean checkMagic(MemoryMappedFile file)
    {
        String magic = getMagic(file);
        return magic.equalsIgnoreCase(Constants.MAGIC);
    }

    private static void clearIndexRWAndCount(MemoryMappedFile indexFile)
    {
        indexFile.putIntVolatile(6, 0);
    }

    public static void beginIndexWrite(MemoryMappedFile indexFile) throws InterruptedException
    {
        // Set the rw flag.
        indexFile.putShortVolatile(6, (short) 1);
        final int maxWaitMs = 10000; //  wait for the reader for 10s.
        final int sleepMs = 10;
        int waitMs = 0;
        while (indexFile.getShortVolatile(8) > 0)
        {
            /**
             * Wait for the existing readers to finish.
             * As rw flag has been set, there will be no new readers,
             * the existing readers should finished cache reading in
             * 10s (10000ms). If the reader can not finish cache reading
             * in 10s, it is considered as failed.
             */
            Thread.sleep(sleepMs);
            waitMs += sleepMs;
            if (waitMs >= maxWaitMs)
            {
                // clear reader count to continue writing.
                indexFile.putShortVolatile(8, (short) 0);
                break;
            }
        }
    }

    public static void endIndexWrite(MemoryMappedFile indexFile)
    {
        indexFile.putShortVolatile(6, (short) 0);
    }

    public static void beginIndexRead(MemoryMappedFile indexFile) throws InterruptedException
    {
        int v = indexFile.getIntVolatile(6);
        short readerCount = indexFile.getShortVolatile(8);
        if (readerCount >= MAX_READER_COUNT)
        {
            throw new InterruptedException("Reaches the max concurrent read count.");
        }
        while ((v & RW_MASK) > 0 ||
        // cas ensures that reading rw flag and increasing reader count is atomic.
        indexFile.compareAndSwapInt(6, v, v+READER_COUNT_INC) == false)
        {
            // We failed to get read lock or increase reader count.
            if ((v & RW_MASK) > 0)
            {
                // if there is an existing writer, sleep for 10ms.
                Thread.sleep(10);
            }
            v = indexFile.getIntVolatile(6);
            readerCount = indexFile.getShortVolatile(8);
            if (readerCount >= MAX_READER_COUNT)
            {
                throw new InterruptedException("Reaches the max concurrent read count.");
            }
        }
    }

    public static void endIndexRead(MemoryMappedFile indexFile)
    {
        int v = indexFile.getIntVolatile(6);
        // if reader count is already <= 0, nothing will be done.
        while ((v & READER_COUNT_MASK) > 0)
        {
            if (indexFile.compareAndSwapInt(6, v, v-READER_COUNT_INC))
            {
                // if v is not changed and the reader count is successfully decreased, break.
                break;
            }
            v = indexFile.getIntVolatile(6);
        }
    }

    public static void setIndexVersion(MemoryMappedFile indexFile, int version)
    {
        indexFile.putIntVolatile(10, version);
    }

    public static int getIndexVersion(MemoryMappedFile indexFile)
    {
        return indexFile.getIntVolatile(10);
    }

    /**
     * Read radix from index file.
     * @param indexFile the index file to be read.
     * @return the radix tree read from index file.
     */
    public static PixelsRadix loadRadixIndex(MemoryMappedFile indexFile) throws CacheException
    {
        PixelsRadix radix = new PixelsRadix();
        readRadix(indexFile, PixelsCacheUtil.INDEX_RADIX_OFFSET, radix.getRoot(), 1);
        return radix;
    }

    /**
     * Read and construct the index from index file.
     * @param indexFile the index file to be read.
     * @param nodeOffset the offset of the current root node of the free (or sub-tree).
     * @param node the current root node to be read from index file.
     * @param level the current level of the node, starts from 1 for root of the tree.
     */
    private static void readRadix(MemoryMappedFile indexFile, long nodeOffset,
                                       RadixNode node, int level) throws CacheException
    {
        long[] children = readNode(indexFile, nodeOffset, node, level);

        if (node.isKey())
        {
            return;
        }

        if (children == null)
        {
            throw new CacheException("Can not read node normally.");
        }

        for (long childId : children)
        {
            // offset is in the lowest 56 bits, the highest 8 bits leader is discarded.
            long childOffset = childId & 0x00FFFFFFFFFFFFFFL;
            RadixNode childNode = new RadixNode();
            readRadix(indexFile, childOffset, childNode, level+1);
            node.addChild(childNode, true);
        }
    }

    /**
     * Read the index node from index file.
     * @param indexFile the index file to be read.
     * @param nodeOffset the offset of this node in index file.
     * @param node the node to be read from index file.
     * @param level the current level of this node.
     * @return the children ids (1 byte leader + 7 bytes offset) of the node.
     */
    private static long[] readNode(MemoryMappedFile indexFile, long nodeOffset,
                                   RadixNode node, int level)
    {
        if (nodeOffset >= indexFile.getSize())
        {
            logger.debug("Offset exceeds index size. Break. Current size: " + nodeOffset);
            return null;
        }
        int dramAccessCounter = 0;
        node.offset = nodeOffset;
        int nodeHeader = indexFile.getInt(nodeOffset);
        dramAccessCounter++;
        int nodeChildrenNum = nodeHeader & 0x000001FF;
        int nodeEdgeSize = (nodeHeader & 0x7FFFFE00) >>> 9;

        byte[] childrenData = new byte[nodeChildrenNum * 8];
        indexFile.getBytes(nodeOffset + 4, childrenData, 0, nodeChildrenNum * 8);
        /**
         * To ensure the consistent endian (big-endian) in Java,
         * we use ByteBuffer to wrap the bytes instead of directly getLong() from indexFile.
         */
        ByteBuffer childrenBuffer = ByteBuffer.wrap(childrenData);
        long[] children = new long[nodeChildrenNum];
        for (int i = 0; i < nodeChildrenNum; ++i)
        {
            children[i] = childrenBuffer.getLong();
        }
        dramAccessCounter++;
        byte[] edge = new byte[nodeEdgeSize];
        indexFile.getBytes(nodeOffset + 4 + nodeChildrenNum * 8, edge, 0, nodeEdgeSize);
        dramAccessCounter++;
        node.setEdge(edge);

        if (((nodeHeader >>> 31) & 1) > 0)
        {
            node.setKey(true);
            // read value
            byte[] idx = new byte[12];
            indexFile.getBytes(nodeOffset + 4 + (nodeChildrenNum * 8) + nodeEdgeSize,
                    idx, 0, 12);
            dramAccessCounter++;
            PixelsCacheIdx cacheIdx = new PixelsCacheIdx(idx);
            cacheIdx.dramAccessCount = dramAccessCounter;
            cacheIdx.radixLevel = level;
            node.setValue(cacheIdx);
        }
        else
        {
            node.setKey(false);
        }

        return children;
    }

    public static void flushRadix(MemoryMappedFile indexFile, PixelsRadix radix)
    {
        // TODO: flush radix is currently implemented in PixelsCacheWriter, to be moved here.
    }

    public static void setCacheStatus(MemoryMappedFile cacheFile, short status)
    {
        cacheFile.putShortVolatile(6, status);
    }

    public static short getCacheStatus(MemoryMappedFile cacheFile)
    {
        return cacheFile.getShortVolatile(6);
    }

    public static void setCacheSize(MemoryMappedFile cacheFile, long size)
    {
        cacheFile.putLongVolatile(8, size);
    }

    public static long getCacheSize(MemoryMappedFile cacheFile)
    {
        return cacheFile.getLongVolatile(8);
    }
}
