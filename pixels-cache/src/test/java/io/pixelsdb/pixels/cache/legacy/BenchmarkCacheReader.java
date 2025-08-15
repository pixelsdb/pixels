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

import com.google.common.util.concurrent.SettableFuture;
import io.pixelsdb.pixels.cache.PixelsCacheConfig;
import io.pixelsdb.pixels.cache.PixelsCacheIdx;
import io.pixelsdb.pixels.cache.PixelsCacheKey;
import io.pixelsdb.pixels.cache.PixelsCacheUtil;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class BenchmarkCacheReader
{
    final int durationMilis = 1000 * 60; // 5 min
    List<PixelsCacheIdx> pixelsCacheIdxs = new ArrayList<>(4096);
    List<PixelsCacheKey> pixelsCacheKeys = new ArrayList<>(4096);

    @Before
    public void prepare()
    {
        Configurator.setRootLevel(Level.DEBUG);
    }

    @Before
    public void loadMockData() throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader("dumpedCache.txt"));
        String line = br.readLine();
        String idxString = "";
        String keyString = "";
        while (line != null)
        {
            keyString = line.split(";")[1];
            idxString = line.split(";")[2];

            String[] keyTokens = keyString.split("-");
            long blkId = Long.parseLong(keyTokens[0]);
            short rgId = Short.parseShort(keyTokens[1]);
            short colId = Short.parseShort(keyTokens[2]);

            String[] idxTokens = idxString.split("-");
            long offset = Long.parseLong(idxTokens[0]);
            int length = Integer.parseInt(idxTokens[1]);
            pixelsCacheIdxs.add(new PixelsCacheIdx(offset, length));
            pixelsCacheKeys.add(new PixelsCacheKey(blkId, rgId, colId));
            line = br.readLine();
        }
    }

    @Before
    public void buildConfig()
    {
        ConfigFactory config = ConfigFactory.Instance();
        // disk cache
//        config.addProperty("cache.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.cache");
        config.addProperty("cache.base.location", "/mnt/nvme1n1/partitioned/pixels.cache");

        config.addProperty("cache.size", String.valueOf(70 * 1024 * 1024 * 1024L)); // 70GiB
        config.addProperty("cache.partitions", "32");


        config.addProperty("index.base.location", "/dev/shm/pixels-partitioned-cache/pixels.index");
//        config.addProperty("index.disk.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned/pixels.index");

        config.addProperty("index.size", String.valueOf(100 * 1024 * 1024)); // 100 MiB

        config.addProperty("cache.storage.scheme", "mock"); // 100 MiB
        config.addProperty("heartbeat.lease.ttl.seconds", "20");
        config.addProperty("heartbeat.period.seconds", "10");
        config.addProperty("cache.absolute.balancer.enabled", "false");
        config.addProperty("cache.enabled", "true");
        config.addProperty("enabled.storage.schemes", "mock");

    }

    @Test // read; w/ complex protocol; partitioned; radix
    public void benchmarkProtocolPartitionedRadixRead() throws Exception
    {
        int nReaders = 8;
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once
        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);
        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheFile(cacheFile).setIndexFile(indexFile).build();
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                Random random = new Random();
                byte[] buf = new byte[4096];
                // metrics
                long start = System.nanoTime();
                long totalBytes = 0;
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
                    int readBytes = reader.get(cacheKey, buf, cacheIdx.length);
                    if (readBytes <= 0)
                    {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
                        totalBytes += readBytes;
                        io++;
                    }
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, totalBytes, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        Thread.sleep(durationMilis);
        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n",
                nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);

    }
    // Note: for cacheReader test, the bandwidth and latency is bounded by the cache content read.
    //       so we can use radix and complex protocol to test the performance

    // Note: xxxx2 means different layout of the index and cache.

    @Test // read; w/ complex protocol; partitioned; radix; disk content reader
    public void benchmarkProtocolPartitionedRadixRead2() throws Exception
    {
        ConfigFactory config = ConfigFactory.Instance();

        config.addProperty("index.base.location", "/dev/shm/pixels-partitioned-cache-2/pixels.index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned-2/pixels.index");
        config.addProperty("cache.base.location", "/mnt/nvme1n1/partitioned-2/pixels.cache");

        int nReaders = 8;
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once
        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheLocation(cacheConfig.getCacheLocation())
                        .setIndexLocation(cacheConfig.getIndexLocation()).build2();
                Random random = new Random(System.nanoTime());
                byte[] buf = new byte[4096];
                // metrics
                long start = System.nanoTime();
                long totalBytes = 0;
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
                    int readBytes = reader.get(cacheKey, buf, cacheIdx.length);
                    if (readBytes <= 0)
                    {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
                        totalBytes += readBytes;
                        io++;
                    }
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, totalBytes, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        Thread.sleep(durationMilis);
        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n", nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);

    }

    @Test // read; w/ complex protocol; partitioned; hash
    public void benchmarkProtocolPartitionedHashRead() throws Exception
    {
        ConfigFactory config = ConfigFactory.Instance();
        config.addProperty("index.base.location", "/dev/shm/pixels-partitioned-cache/pixels.hash-index");
//        config.addProperty("index.disk.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.hash-index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned/pixels.hash-index");

        int nReaders = 8;
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once
        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);
        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            PartitionCacheReader reader = PartitionCacheReader.newBuilder().setIndexType("hash").setCacheFile(cacheFile).setIndexFile(indexFile).build();
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                Random random = new Random();
                byte[] buf = new byte[4096];
                // metrics
                long start = System.nanoTime();
                long totalBytes = 0;
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
                    int readBytes = reader.get(cacheKey, buf, cacheIdx.length);
                    if (readBytes <= 0)
                    {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
                        totalBytes += readBytes;
                        io++;
                    }
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, totalBytes, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        Thread.sleep(durationMilis);
        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n", nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);

    }

    @Test // read; w/ optimized protocol(no reader count); partitioned
    public void benchmarkSimpleProtocolPartitionedRead() throws Exception
    {
        int nReaders = 8;
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once
        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);
        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheFile(cacheFile).setIndexFile(indexFile).build();
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                Random random = new Random();
                byte[] buf = new byte[4096];
                // metrics
                long start = System.nanoTime();
                long totalBytes = 0;
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
                    int readBytes = reader.simpleget(cacheKey, buf, cacheIdx.length);
                    if (readBytes <= 0)
                    {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
                        totalBytes += readBytes;
                        io++;
                    }
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, totalBytes, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        Thread.sleep(durationMilis);
        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n", nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);

    }

    @Test // read; w/o protocol; partitioned; radix
    public void benchmarkPartitionedRead() throws Exception
    {
        int nReaders = 8;
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once
        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);
        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheFile(cacheFile).setIndexFile(indexFile).build();
                Random random = new Random();
                byte[] buf = new byte[4096];
                // metrics
                long start = System.nanoTime();
                long totalBytes = 0;
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
                    int readBytes = reader.naiveget(cacheKey, buf, cacheIdx.length);
                    if (readBytes <= 0)
                    {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
                        totalBytes += readBytes;
                        io++;
                    }
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, totalBytes, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        Thread.sleep(durationMilis);
        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n", nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);

    }

    @Test // read; w/o protocol; single; radix; mmap content reader
    public void benchmarkSinglePartitionRead() throws Exception
    {
        int nReaders = 8;
        MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.index", 102400000);
        MemoryMappedFile cacheFile = new MemoryMappedFile("/mnt/nvme1n1/pixels.cache", 70L * 1024 * 1024 * 1024);
//        MemoryMappedFile cacheFile = new MemoryMappedFile("/mnt/nvme1n1/partitioned/pixels.cache", 70L * 1024 * 1024 * 1024);

        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {

            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                CacheIndexReader indexReader = new RadixIndexReader(indexFile);
//            CacheContentReader contentReader = new DiskCacheContentReader("/mnt/nvme1n1/pixels.cache");
//            CacheContentReader contentReader = new DiskCacheContentReader("/mnt/nvme1n1/partitioned/pixels.cache");
//
                CacheContentReader contentReader = new MmapFileCacheContentReader(cacheFile);

                SimpleCacheReader reader = new SimpleCacheReader(indexReader, contentReader);
                Random random = new Random();
                byte[] buf = new byte[4096];
                // metrics
                long start = System.nanoTime();
                long totalBytes = 0;
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
                    int readBytes = reader.naiveget(cacheKey, buf, cacheIdx.length);
                    if (readBytes <= 0)
                    {
                        System.out.println("readBytes=0 " + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
//                        byte ele = buf[0];
//                        System.out.println(ele + " " + cacheIdx.length);
//                        for (int j = 0; j < cacheIdx.length; ++j) assert(buf[j] == ele);
                        totalBytes += readBytes;
                        io++;
                    }
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, totalBytes, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        Thread.sleep(durationMilis);
        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n", nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);

    }

    @Test // read; w/o protocol; single; radix; disk content reader; the bandwidth is doubled than mmap content reader
    public void benchmarkSinglePartitionRead2() throws Exception
    {
        int nReaders = 8;
        MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.index", 102400000);
        MemoryMappedFile cacheFile = new MemoryMappedFile("/mnt/nvme1n1/pixels.cache", 70L * 1024 * 1024 * 1024);
//        MemoryMappedFile cacheFile = new MemoryMappedFile("/mnt/nvme1n1/partitioned/pixels.cache", 70L * 1024 * 1024 * 1024);

        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {

            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                CacheIndexReader indexReader = new RadixIndexReader(indexFile);
                CacheContentReader contentReader = new DiskCacheContentReader("/mnt/nvme1n1/pixels.cache");

                SimpleCacheReader reader = new SimpleCacheReader(indexReader, contentReader);
                Random random = new Random();
                byte[] buf = new byte[4096];
                // metrics
                long start = System.nanoTime();
                long totalBytes = 0;
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
                    int readBytes = reader.naiveget(cacheKey, buf, cacheIdx.length);
                    if (readBytes <= 0)
                    {
                        System.out.println("readBytes=0 " + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
//                        byte ele = buf[0];
//                        System.out.println(ele + " " + cacheIdx.length);
//                        for (int j = 0; j < cacheIdx.length; ++j) assert(buf[j] == ele);
                        totalBytes += readBytes;
                        io++;
                    }
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, totalBytes, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        Thread.sleep(durationMilis);
        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n", nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);

    }

    @Test // readers + 1 writer; w/ protocol; partitioned; radix; mmap content reader
    public void benchmarkProtocolReadAndWrite() throws Exception
    {
        int nReaders = 8;
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once
        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);
        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheFile(cacheFile).setIndexFile(indexFile).build();
                Random random = new Random();
                byte[] buf = new byte[1024 * 1024];
                // metrics
                long start = System.nanoTime();
                long totalBytes = 0;
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
                    int readBytes = reader.get(cacheKey, buf, cacheIdx.length);
                    if (readBytes <= 0)
                    {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
                        totalBytes += readBytes;
                        io++;
                    }
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, totalBytes, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(false) // dont overwrite
                .setWriteContent(true)
                .setHostName("yeeef-fort")
                .setCacheConfig(cacheConfig)
                .build();

        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnChunkOrders
        Set<String> cacheColumnChunkOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        long startWrite = System.currentTimeMillis();
        assert (writer.incrementalLoad(623, new ArrayList<>(cacheColumnChunkOrders), files.toArray(new String[0])) == 0);

        finish.set(1);
        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n", nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);
        System.out.printf("write bandwidth=%f%n", 64 * 1024.0 / ((System.currentTimeMillis() - startWrite) / 1000.0));


    }

    @Test // readers + 1 writer; w/ protocol; partitioned; radix; disk content reader
    public void benchmarkProtocolReadAndWrite2() throws Exception
    {
        ConfigFactory config = ConfigFactory.Instance();

        config.addProperty("index.base.location", "/dev/shm/pixels-partitioned-cache-2/pixels.index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned-2/pixels.index");
        config.addProperty("cache.base.location", "/mnt/nvme1n1/partitioned-2/pixels.cache");

        int nReaders = 6;
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheLocation(cacheConfig.getCacheLocation())
                        .setIndexLocation(cacheConfig.getIndexLocation()).build2();
//                Random random = new Random(System.nanoTime());
                Random random = new Random();

                byte[] buf = new byte[1024 * 1024];
                // metrics
                long start = System.nanoTime();
                long totalBytes = 0;
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    while (buf.length < cacheIdx.length) buf = new byte[buf.length * 2];
                    int readBytes = reader.simpleget(cacheKey, buf, cacheIdx.length);
                    if (readBytes <= 0)
                    {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
                        totalBytes += readBytes;
                        io++;
                    }
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, totalBytes, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(false) // dont overwrite
                .setWriteContent(true)
                .setHostName("yeeef-fort")
                .setCacheConfig(cacheConfig)
                .build2();

        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnChunkOrders
        Set<String> cacheColumnChunkOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        long startWrite = System.currentTimeMillis();

        assert (writer.incrementalLoad(900, new ArrayList<>(cacheColumnChunkOrders), files.toArray(new String[0])) == 0);
//        assert (writer.incrementalLoad(800, new ArrayList<>(cacheColumnChunkOrders), files.toArray(new String[0])) == 0);
//        assert (writer.incrementalLoad(801, new ArrayList<>(cacheColumnChunkOrders), files.toArray(new String[0])) == 0);


        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n", nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);
        System.out.printf("write bandwidth=%f%n", 64 * 1 * 1024.0 / ((System.currentTimeMillis() - startWrite) / 1000.0));


    }

    @Test // readers + 1 writer; w/ protocol; partitioned; hash
    public void benchmarkProtocolReadAndWriteIndex() throws Exception
    {
        ConfigFactory config = ConfigFactory.Instance();
        config.addProperty("index.base.location", "/dev/shm/pixels-partitioned-cache/pixels.hash-index");
//        config.addProperty("index.disk.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.hash-index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned/pixels.hash-index");
        int nReaders = 8;
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once
        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);
        SettableFuture<Integer> finish = SettableFuture.create();
        List<Future<BenchmarkResult>> futures = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = readExecutor.submit(() -> {
                PartitionCacheReader reader = PartitionCacheReader.newBuilder().setIndexType("hash").setCacheFile(cacheFile).setIndexFile(indexFile).build();
                Random random = new Random();
                // metrics
                long start = System.nanoTime();
                long io = 0;
                while (!finish.isDone())
                {
                    int index = random.nextInt(pixelsCacheKeys.size());
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx readed = reader.search(cacheKey);
                    if (readed == null)
                    {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    } else
                    {
                        io++;
                    }
//                    if (io % 10000 == 0) System.out.println(io);
                }
                long end = System.nanoTime();
                double elapsed = (double) (end - start) / 1e6;

                BenchmarkResult result = new BenchmarkResult(io, 0, elapsed);
                System.out.println(result);
                return result;
            });
            futures.add(future);
        }

        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setIndexType("hash")
                .setOverwrite(false) // dont overwrite
                .setWriteContent(false)
                .setHostName("yeeef-fort")
                .setCacheConfig(cacheConfig)
                .build();

        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnChunkOrders
        Set<String> cacheColumnChunkOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        long startWrite = System.currentTimeMillis();
        assert (writer.incrementalLoad(800, new ArrayList<>(cacheColumnChunkOrders), files.toArray(new String[0])) == 0);

        finish.set(1);
        List<BenchmarkResult> results = new ArrayList<>(nReaders);
        for (int i = 0; i < nReaders; ++i)
        {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results)
        {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= nReaders;
        System.out.printf("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms%n", nReaders, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency);
        System.out.printf("write bandwidth=%f%n", 64 * 1024.0 / ((System.currentTimeMillis() - startWrite) / 1000.0));


    }

    static class BenchmarkResult
    {
        double elapsed;
        double totalBytes;
        double totalIO;
        double iops;
        double bandwidthMb;
        double bandwidthMib;

        double latency;

        BenchmarkResult(double totalIO, double totalBytes, double elapsedInMili)
        {
            this.elapsed = elapsedInMili;
            this.totalBytes = totalBytes;
            this.totalIO = totalIO;
            this.iops = totalIO / (elapsed / 1e3);
            this.bandwidthMb = (totalBytes / 1000.0 / 1000.0) / (elapsed / 1e3);
            this.bandwidthMib = (totalBytes / 1024.0 / 1024.0) / (elapsed / 1e3);
            this.latency = 1.0 / this.iops * 1000;
        }

        @Override
        public String toString()
        {
            return String.format("elapsed=%fms(%fs), IOPS=%f, bandwidth=%fMB/s(%fMiB/s), latency=%fms, totalIO=%f, totalBytes=%fGiB",
                    elapsed, elapsed / 1e3, iops, bandwidthMb, bandwidthMib, latency, totalIO, totalBytes / 1024.0 / 1024.0 / 1024.0);
        }

    }
}
