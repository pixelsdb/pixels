package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.utils.Constants;
import org.apache.directory.api.util.Strings;

import java.nio.charset.StandardCharsets;

/**
 * pixels cache header
 * index:
 *     - HEADER: MAGIC(6 bytes), RW(2 bytes), VERSION(4 bytes), READER_COUNT(4 bytes)
 *     - RADIX
 * cache:
 *     - HEADER: MAGIC(6 bytes), STATUS(2 bytes), SIZE(8 bytes)
 *     - CONTENT
 *
 * @author guodong
 */
public class PixelsCacheUtil
{
    private static final int MAX_READER_COUNT = 2 ^ 31 - 1;
    public static final int INDEX_RADIX_OFFSET = 16;

    public enum RWFlag {
        READ((short) 0), WRITE((short) 1);

        private final short id;
        RWFlag(short id)
        {
            this.id = id;
        }

        public short getId()
        {
            return id;
        }
    }

    public enum CacheStatus {
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
        setIndexRW(indexFile, RWFlag.READ.getId());
        setIndexVersion(indexFile, 0);
        setIndexReaderCount(indexFile, 0);
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

    public static void setIndexRW(MemoryMappedFile indexFile, short rwFlag)
    {
        indexFile.putShortVolatile(6, rwFlag);
    }

    public static short getIndexRW(MemoryMappedFile indexFile)
    {
        return indexFile.getShortVolatile(6);
    }

    public static void setIndexVersion(MemoryMappedFile indexFile, int version)
    {
        indexFile.putIntVolatile(8, version);
    }

    public static int getIndexVersion(MemoryMappedFile indexFile)
    {
        return indexFile.getIntVolatile(8);
    }

    public static void setIndexReaderCount(MemoryMappedFile indexFile, int readerCount)
    {
        indexFile.putIntVolatile(12, readerCount);
    }

    public static int getIndexReaderCount(MemoryMappedFile indexFile)
    {
        return indexFile.getIntVolatile(12);
    }

    public static boolean indexReaderCountIncrement(MemoryMappedFile indexFile)
    {
        int count = getIndexReaderCount(indexFile) + 1;
        if (count <= MAX_READER_COUNT) {
            return indexFile.compareAndSwapInt(12, count - 1, count);
        }
        return false;
    }

    public static boolean indexReaderCountDecrement(MemoryMappedFile indexFile)
    {
        int count = getIndexReaderCount(indexFile) - 1;
        if (count >= 0) {
            return indexFile.compareAndSwapInt(12, count + 1, count);
        }
        return false;
    }

    public static PixelsRadix getIndexRadix(MemoryMappedFile indexFile)
    {
        // todo read radix from index file
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
