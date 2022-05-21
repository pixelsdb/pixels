package io.pixelsdb.pixels.cache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class BenchmarkCacheIndex {
    static int KEYS = 512000;
    static int READ_COUNT = KEYS * 50;
    MemoryMappedFile bigEndianIndexFile;
    MemoryMappedFile littleEndianIndexFile;
    MemoryMappedFile hashIndexFile;
    RandomAccessFile hashIndexDiskFile;


    PixelsCacheKey[] pixelsCacheKeys = new PixelsCacheKey[KEYS];

    @Before
    public void init() {
        try {
            bigEndianIndexFile = new MemoryMappedFile("/dev/shm/pixels.index.bak", 102400000);
            littleEndianIndexFile = new MemoryMappedFile("/dev/shm/pixels.index", 102400000);
            hashIndexFile = new MemoryMappedFile("/dev/shm/pixels.hash-index", 102400000);
            hashIndexDiskFile = new RandomAccessFile("/dev/shm/pixels.hash-index", "r");

            BufferedReader br = new BufferedReader(new FileReader("tmp.txt"));
            String line = br.readLine();
            int ptr = 0;
            while (line != null) {
                line = line.split(";")[1];
                String[] tokens = line.split("-");
                long blockId = Long.parseLong(tokens[0]);
                short rowGroupId = Short.parseShort(tokens[1]);
                short columnId = Short.parseShort(tokens[2]);
                pixelsCacheKeys[ptr] = new PixelsCacheKey(blockId, rowGroupId, columnId);
                ptr += 1;
                line = br.readLine();
            }
            System.out.println(Arrays.toString(Arrays.copyOfRange(pixelsCacheKeys, 0, 10)));

        } catch (Exception e) {

            e.printStackTrace();
        }

    }

    @Test
    public void searchAllKeys() throws InterruptedException {
        int threadNum = 8;
//        Random random = new Random(System.nanoTime());
        Random random = new Random(233);

        Thread[] threads = new Thread[threadNum];

        for (int i = 0; i < threadNum; i++)
        {
            int[] accesses = new int[READ_COUNT];
            for (int k = 0; k < READ_COUNT; k++)
            {
                accesses[k] = random.nextInt(READ_COUNT) % KEYS;
            }
            threads[i] = new Thread(new CacheSearcher(pixelsCacheKeys, accesses, bigEndianIndexFile));
        }

        for (int i = 0; i < threadNum; i++)
        {
            threads[i].start();
        }
        for (int i = 0; i < threadNum; i++)
        {
            threads[i].join();
        }

    }

    @Test
    public void nativeSearchAllKeys() throws InterruptedException {
        int threadNum = 8;
        Random random = new Random(System.nanoTime());

        Thread[] threads = new Thread[threadNum];

        for (int i = 0; i < threadNum; i++)
        {
            int[] accesses = new int[READ_COUNT];
            for (int k = 0; k < READ_COUNT; k++)
            {
                accesses[k] = random.nextInt(READ_COUNT) % KEYS;
            }
            threads[i] = new Thread(new NativeCacheSearcher(pixelsCacheKeys, accesses, littleEndianIndexFile));
        }

        for (int i = 0; i < threadNum; i++)
        {
            threads[i].start();
        }
        for (int i = 0; i < threadNum; i++)
        {
            threads[i].join();
        }

    }

    @Test
    public void hashSearchAllKeys() throws InterruptedException {
        int threadNum = 8;
//        Random random = new Random(System.nanoTime());
        Random random = new Random(233);

        Thread[] threads = new Thread[threadNum];

        for (int i = 0; i < threadNum; i++)
        {
            int[] accesses = new int[READ_COUNT];
            for (int k = 0; k < READ_COUNT; k++)
            {
                accesses[k] = random.nextInt(READ_COUNT) % KEYS;
            }
            threads[i] = new Thread(new HashCacheSearcher(pixelsCacheKeys, accesses, hashIndexFile));
        }

        for (int i = 0; i < threadNum; i++)
        {
            threads[i].start();
        }
        for (int i = 0; i < threadNum; i++)
        {
            threads[i].join();
        }

    }

    @Test
    public void hashDiskSearchAllKeys() throws InterruptedException, FileNotFoundException {
        int threadNum = 8;
//        Random random = new Random(System.nanoTime());
        Random random = new Random(233);

        Thread[] threads = new Thread[threadNum];

        for (int i = 0; i < threadNum; i++)
        {
            int[] accesses = new int[READ_COUNT];
            for (int k = 0; k < READ_COUNT; k++)
            {
                accesses[k] = random.nextInt(READ_COUNT) % KEYS;
            }
            // Note: we have to create a new randomAccessFile per thread, otherwise seek will influence each other
            threads[i] = new Thread(new HashCacheDiskSearcher(pixelsCacheKeys, accesses, new RandomAccessFile("/dev/shm/pixels.hash-index", "r")));
        }

        for (int i = 0; i < threadNum; i++)
        {
            threads[i].start();
        }
        for (int i = 0; i < threadNum; i++)
        {
            threads[i].join();
        }

    }

    static class NativeCacheSearcher implements Runnable
    {
        private final int[] idxes;
        private final MemoryMappedFile indexFile;
        private final PixelsCacheKey[] pixelsCacheKeys;

        NativeCacheSearcher(PixelsCacheKey[] pixelsCacheKeys, int[] idxes, MemoryMappedFile indexFile)
        {
            this.pixelsCacheKeys = pixelsCacheKeys;
            this.idxes = idxes;
            this.indexFile = indexFile;
        }

        @Override
        public void run()
        {
            PixelsNativeCacheReader cacheReader = PixelsNativeCacheReader.newBuilder()
                    .setCacheFile(null)
                    .setIndexFile(indexFile)
                    .build();
            int totalAcNum = 0;
            int totalLevel = 0;
            long searchStart = System.nanoTime();
            for (int i = 0; i < idxes.length; i++)
            {
                PixelsCacheKey cacheKey = pixelsCacheKeys[idxes[i]];
                PixelsCacheIdx idx = cacheReader.search(cacheKey.blockId,
                        cacheKey.rowGroupId,
                        cacheKey.columnId);
                if (idx == null)
                {
                    System.out.println("[error] cannot find " + cacheKey.blockId
                            + "-" + cacheKey.rowGroupId
                            + "-" + cacheKey.columnId);
                }
                else
                {
                    totalAcNum += idx.dramAccessCount;
                    totalLevel += idx.radixLevel;
                }
            }
            long searchEnd = System.nanoTime();
            System.out.println("[thread search]: total access=" + totalAcNum +
                    ", elapsed=" + (double) (searchEnd - searchStart)/1e6 + "ms" +
                    " kps=" + READ_COUNT / ((double) (searchEnd - searchStart)/1e9)+ " total level=" + totalLevel);
        }
    }

    static class HashCacheSearcher implements Runnable
    {
        private final int[] idxes;
        private final MemoryMappedFile indexFile;
        private final PixelsCacheKey[] pixelsCacheKeys;

        HashCacheSearcher(PixelsCacheKey[] pixelsCacheKeys, int[] idxes, MemoryMappedFile indexFile)
        {
            this.pixelsCacheKeys = pixelsCacheKeys;
            this.idxes = idxes;
            this.indexFile = indexFile;
        }

        @Override
        public void run()
        {
            HashIndexReader cacheReader = new HashIndexReader(indexFile);
            int totalAcNum = 0;
            int totalLevel = 0;
            long searchStart = System.nanoTime();
            for (int i = 0; i < idxes.length; i++)
            {
                PixelsCacheKey cacheKey = pixelsCacheKeys[idxes[i]];
                PixelsCacheIdx idx = cacheReader.nativeSearch(cacheKey.blockId,
                        cacheKey.rowGroupId,
                        cacheKey.columnId);
                if (idx == null)
                {
                    System.out.println("[error] cannot find " + cacheKey.blockId
                            + "-" + cacheKey.rowGroupId
                            + "-" + cacheKey.columnId);
                }
                else
                {
                    totalAcNum += idx.dramAccessCount;
                    totalLevel += idx.radixLevel;
                }
            }
            long searchEnd = System.nanoTime();
            System.out.println("[thread search]: total access=" + totalAcNum +
                    ", elapsed=" + (double) (searchEnd - searchStart)/1e6 + "ms" +
                    " kps=" + READ_COUNT / ((double) (searchEnd - searchStart)/1e9)+ " total level=" + totalLevel);
        }
    }

    static class HashCacheDiskSearcher implements Runnable
    {
        private final int[] idxes;
        private final RandomAccessFile indexFile;
        private final PixelsCacheKey[] pixelsCacheKeys;

        HashCacheDiskSearcher(PixelsCacheKey[] pixelsCacheKeys, int[] idxes, RandomAccessFile indexFile)
        {
            this.pixelsCacheKeys = pixelsCacheKeys;
            this.idxes = idxes;
            this.indexFile = indexFile;
        }

        @Override
        public void run()
        {
            try {
                HashIndexDiskReader cacheReader = new HashIndexDiskReader(indexFile);
                int totalAcNum = 0;
                int totalLevel = 0;
                long searchStart = System.nanoTime();
                for (int i = 0; i < idxes.length; i++)
                {
                    PixelsCacheKey cacheKey = pixelsCacheKeys[idxes[i]];
                    PixelsCacheIdx idx = cacheReader.search(cacheKey.blockId,
                            cacheKey.rowGroupId,
                            cacheKey.columnId);
                    if (idx == null)
                    {
                        System.out.println("[error] cannot find " + cacheKey.blockId
                                + "-" + cacheKey.rowGroupId
                                + "-" + cacheKey.columnId);
                    }
                    else
                    {
                        totalAcNum += idx.dramAccessCount;
                        totalLevel += idx.radixLevel;
                    }
                }
                long searchEnd = System.nanoTime();
                System.out.println("[thread search]: total access=" + totalAcNum +
                        ", elapsed=" + (double) (searchEnd - searchStart)/1e6 + "ms" +
                        " kps=" + READ_COUNT / ((double) (searchEnd - searchStart)/1e9)+ " total level=" + totalLevel);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    static class CacheSearcher implements Runnable
    {
        private final int[] idxes;
        private final MemoryMappedFile indexFile;
        private final PixelsCacheKey[] pixelsCacheKeys;

        CacheSearcher(PixelsCacheKey[] pixelsCacheKeys, int[] idxes, MemoryMappedFile indexFile)
        {
            this.pixelsCacheKeys = pixelsCacheKeys;
            this.idxes = idxes;
            this.indexFile = indexFile;
        }

        @Override
        public void run()
        {
            PixelsCacheReader cacheReader = PixelsCacheReader.newBuilder()
                    .setCacheFile(null)
                    .setIndexFile(indexFile)
                    .build();
            int totalAcNum = 0;
            int totalLevel = 0;
            long searchStart = System.nanoTime();
            for (int i = 0; i < idxes.length; i++)
            {
                PixelsCacheKey cacheKey = pixelsCacheKeys[idxes[i]];
                PixelsCacheIdx idx = cacheReader.search(cacheKey.blockId,
                        cacheKey.rowGroupId,
                        cacheKey.columnId);
                if (idx == null)
                {
                    System.out.println("[error] cannot find " + cacheKey.blockId
                            + "-" + cacheKey.rowGroupId
                            + "-" + cacheKey.columnId);
                }
                else
                {
                    totalAcNum += idx.dramAccessCount;
                    totalLevel += idx.radixLevel;
                }
            }
            long searchEnd = System.nanoTime();
            System.out.println("[thread search]: total access=" + totalAcNum +
                    ", elapsed=" + (double) (searchEnd - searchStart)/1e6 + "ms" +
                    " kps=" + READ_COUNT / ((double) (searchEnd - searchStart)/1e9)+ " total level=" + totalLevel);
        }
    }

}
