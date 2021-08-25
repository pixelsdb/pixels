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
package io.pixelsdb.pixels.test;

import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.cache.PixelsCacheIdx;
import io.pixelsdb.pixels.cache.PixelsCacheKey;
import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.exception.FSException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * pixels
 * java -jar xxx.jar hostname metahost layout_version thread_num
 * java -jar pixels-tools-0.1.0-SNAPSHOT-full.jar dbiir24 dbiir27 3 1
 *
 * @author guodong
 */
public class CacheIndexPerf
{
    private static ConfigFactory config = ConfigFactory.Instance();
    private static PixelsCacheKey[] pixelsCacheKeys;
    private static List<String> cachedColumnlets;
    private static List<String> cachedPaths = new ArrayList<>();

    public static void main(String[] args)
    {
        try
        {
            long prepareStart = System.currentTimeMillis();
            CacheIndexPerf cacheIndexPerf = new CacheIndexPerf();
            cacheIndexPerf.prepare(args[0], args[1], Integer.parseInt(args[2]));
//            cacheIndexPerf.prepare("dbiir24", "dbiir27", 3);
            int threadNum = Integer.parseInt(args[3]);
//            int threadNum = 1;

            Thread[] threads = new Thread[threadNum];
            int readCount = cachedColumnlets.size() * cachedPaths.size();
            pixelsCacheKeys = new PixelsCacheKey[readCount];

            MemoryMappedFile indexFile = new MemoryMappedFile(config.getProperty("index.location"),
                    Long.parseLong(config.getProperty("index.size")));
//            MemoryMappedFile indexFile = new MemoryMappedFile("/home/guod/Desktop/pixels.index", 1024*1024*1024);

            int idx = 0;
            for (String path : cachedPaths)
            {
                for (int i = 0; i < cachedColumnlets.size(); i++)
                {
                    String[] columnletIdSplits = cachedColumnlets.get(i).split(":");
                    PixelsCacheKey cacheKey = new PixelsCacheKey(-1,
                            Short.parseShort(columnletIdSplits[0]),
                            Short.parseShort(columnletIdSplits[1]));
                    pixelsCacheKeys[idx++] = cacheKey;
                }
            }

            for (int i = 0; i < threadNum; i++)
            {
                int[] accesses = new int[readCount];
                Random random = new Random(System.nanoTime());
                for (int k = 0; k < readCount; k++)
                {
                    accesses[k] = random.nextInt(readCount);
                }
                threads[i] = new Thread(new CacheSearcher(accesses, indexFile));
            }
            long prepareEnd = System.currentTimeMillis();
            System.out.println("[prepare]: " + (prepareEnd - prepareStart));

            long searchStart = System.nanoTime();
            for (int i = 0; i < threadNum; i++)
            {
                threads[i].start();
            }
            for (int i = 0; i < threadNum; i++)
            {
                threads[i].join();
            }
            long searchEnd = System.nanoTime();
            System.out.println("[search]: " + (searchEnd - searchStart));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    // prepare correct answers
    private void prepare(String hostName, String metaHost, int layoutVersion)
            throws MetadataException, FSException, IOException
    {
        MetadataService metadataService = new MetadataService(metaHost, 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", layoutVersion);
        cachedColumnlets =
                layout.getCompactObject().getColumnletOrder().subList(0, layout.getCompactObject().getCacheBorder());
//        Configuration hdfsConfig = new Configuration();
//        hdfsConfig.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
//        hdfsConfig.set("fs.file.impl", LocalFileSystem.class.getName());
//        try
//        {
//            Path basePath = new Path(layout.getCompactPath());
//            FileSystem fs = FileSystem.get(basePath.toUri(), hdfsConfig);
//            FSFactory fsFactory = new FSFactory(fs);
//            List<Path> paths = fsFactory.listFiles(layout.getCompactPath());
//
//            for (Path path : paths)
//            {
//                if (fsFactory.getBlockLocations(path, 0, Long.MAX_VALUE).get(0).getHostText().equalsIgnoreCase(hostName))
//                {
//                    cachedPaths.add(path);
//                }
//            }
//        }
//        catch (IOException e)
//        {
//            e.printStackTrace();
//        }
        Storage storage = StorageFactory.Instance().getStorage("hdfs");
        List<String> paths = storage.listPaths(layout.getCompactPath());
        for (String path : paths)
        {
            if (storage.getHosts(path)[0].equalsIgnoreCase(hostName))
            {
                cachedPaths.add(path);
            }
        }
    }

    static class CacheSearcher implements Runnable
    {
        private final int[] idxes;
        private final MemoryMappedFile indexFile;

        CacheSearcher(int[] idxes, MemoryMappedFile indexFile)
        {
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
            System.out.println("[thread search]: " + totalAcNum + "," + (searchEnd - searchStart) + "," + totalLevel);
        }
    }
}

