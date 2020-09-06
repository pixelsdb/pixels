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

import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.directory.api.util.Strings;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * pixels cache header
 * index:
 * - HEADER: MAGIC(6 bytes), RW(2 bytes), VERSION(4 bytes), READER_COUNT(4 bytes)
 * - RADIX
 * cache:
 * - HEADER: MAGIC(6 bytes), STATUS(2 bytes), SIZE(8 bytes)
 * - CONTENT
 *
 * @author guodong
 */
public class PixelsCacheUtil
{
    public static final int MAX_READER_COUNT = 2 ^ 15 - 1;

    public static final int RW_MASK;
    public static final int READER_COUNT_MASK;
    public static final int READER_COUNT_INC;
    /**
     * We only use the first 12 bytes in the index, but we start radix tree
     * from offset 16 for memory alignment.
     */
    public static final int INDEX_RADIX_OFFSET = 16;

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

    public static PixelsRadix getIndexRadix(MemoryMappedFile indexFile)
    {
        // TODO: read radix from index file.
        return new PixelsRadix();
    }

    public static void flushRadix(MemoryMappedFile indexFile, PixelsRadix radix)
    {
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
