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
package io.pixelsdb.pixels.cache;

import com.google.common.util.concurrent.SettableFuture;
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
import java.util.stream.Collectors;

public class TestPartitionCacheWriter {

    List<PixelsCacheIdx> pixelsCacheIdxs = new ArrayList<>(4096);
    List<PixelsCacheKey> pixelsCacheKeys = new ArrayList<>(4096);

    @Before
    public void prepare() {
//        LogManager.getRootLogger().atDebug();
        Configurator.setRootLevel(Level.DEBUG);
    }

    @Before
    public void loadMockData() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("dumpedCache.txt"));
        String line = br.readLine();
        String idxString = "";
        String keyString = "";
        while (line != null) {
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
    public void buildConfig() {
        ConfigFactory config = ConfigFactory.Instance();
        // disk cache
//        config.addProperty("cache.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.cache");
        config.addProperty("cache.location", "/mnt/nvme1n1/partitioned/pixels.cache");

        config.addProperty("cache.size", String.valueOf(70 * 1024 * 1024 * 1024L)); // 70GiB
        config.addProperty("cache.partitions", "32");


        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache/pixels.index");
//        config.addProperty("index.disk.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned/pixels.index");

        config.addProperty("index.size", String.valueOf(100 * 1024 * 1024)); // 100 MiB

        config.addProperty("cache.storage.scheme", "mock"); // 100 MiB
        config.addProperty("cache.schema", "pixels");
        config.addProperty("cache.table", "test_mock");
        config.addProperty("lease.ttl.seconds", "20");
        config.addProperty("heartbeat.period.seconds", "10");
        config.addProperty("enable.absolute.balancer", "false");
        config.addProperty("cache.enabled", "true");
        config.addProperty("enabled.storage.schemes", "mock");

    }

    @Test
    public void testBuild() throws Exception {

        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(true)
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .build();
    }

    @Test
    public void testNotOverwriteBuild() throws Exception {
        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(false)
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .build();

    }

    void testBulkLoadIndex(String indexType) throws Exception {
        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(true)
                .setWriteContent(false)
                .setIndexType(indexType)
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .build();
        // construct the layout and files
        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnletOrders
        Set<String> cacheColumnletOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        assert(writer.bulkLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0])) == 0);

        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

//        MemoryMappedFile indexFile = new MemoryMappedFile(config.getIndexDiskLocation(), realIndexSize);
        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);

        PartitionCacheReader reader = PartitionCacheReader.newBuilder().setIndexType(indexType)
                .setCacheFile(cacheFile).setIndexFile(indexFile).build();
        // search the key
        for (int index = 0; index < pixelsCacheIdxs.size(); ++index) {
            PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
            PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);

            // the offset is expected to be different. since this offset is based on the original cache, we can use
            // length as an indicator
            PixelsCacheIdx readCacheIdx = reader.search(cacheKey);
            if (readCacheIdx != null) {
                assert (cacheIdx.length == reader.search(cacheKey).length);
            } else {
                ByteBuffer keyBuf = ByteBuffer.allocate(4);
                keyBuf.putShort(cacheKey.rowGroupId);
                keyBuf.putShort(cacheKey.columnId);
                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                System.out.println(partition + " " + index + " " + cacheKey + " " + cacheIdx);

            }
        }


    }

    void testBulkLoadIndex2(String indexType) throws Exception {
        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(true)
                .setWriteContent(false)
                .setIndexType(indexType)
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .build2();
        // construct the layout and files
        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnletOrders
        Set<String> cacheColumnletOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        assert(writer.bulkLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0])) == 0);

        PartitionCacheReader reader = PartitionCacheReader.newBuilder().setIndexType(indexType)
                .setCacheLocation(cacheConfig.getCacheLocation()).setIndexLocation(cacheConfig.getIndexLocation()).build2();
        // search the key
        for (int index = 0; index < pixelsCacheIdxs.size(); ++index) {
            PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
            PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);

            // the offset is expected to be different. since this offset is based on the original cache, we can use
            // length as an indicator
            PixelsCacheIdx readCacheIdx = reader.search(cacheKey);
            if (readCacheIdx != null) {
                assert (cacheIdx.length == reader.search(cacheKey).length);
            } else {
                ByteBuffer keyBuf = ByteBuffer.allocate(4);
                keyBuf.putShort(cacheKey.rowGroupId);
                keyBuf.putShort(cacheKey.columnId);
                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                System.out.println(partition + " " + index + " " + cacheKey + " " + cacheIdx);

            }
        }


    }

    void testBulkLoadIndexAndContent(String indexType) throws Exception {
        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(true)
                .setWriteContent(true)
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .setIndexType(indexType)
                .build();
        // construct the layout and files
        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnletOrders
        Set<String> cacheColumnletOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        assert(writer.bulkLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0])) == 0);

        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

//        MemoryMappedFile indexFile = new MemoryMappedFile(config.getIndexDiskLocation(), realIndexSize);
        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);

        PartitionCacheReader reader = PartitionCacheReader.newBuilder()
                .setCacheFile(cacheFile).setIndexFile(indexFile).setIndexType(indexType).build();
        // search the key
        byte[] buf = new byte[40960];
        for (int index = 0; index < pixelsCacheIdxs.size(); ++index) {
            PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
            PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);

            // the offset is expected to be different. since this offset is based on the original cache, we can use
            // length as an indicator
            if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
            int readBytes = reader.get(cacheKey, buf, cacheIdx.length);
            if (readBytes == 0) {
                ByteBuffer keyBuf = ByteBuffer.allocate(4);
                keyBuf.putShort(cacheKey.rowGroupId);
                keyBuf.putShort(cacheKey.columnId);
                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                System.out.println("readBytes=0 " + partition + " " + index + " " + cacheKey + " " + cacheIdx);
            } else {
                byte ele = buf[0];
                for (int j = 0; j < readBytes; ++j) {
                    if (ele != buf[j])  {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("corrupted cache column chunk " + partition + " " + index + " " + cacheKey + " " + cacheIdx);
                        break;
                    }
                }
            }
        }

    }

    void testBulkLoadIndexAndContent2(String indexType) throws Exception {
        ConfigFactory config = ConfigFactory.Instance();

        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache-2/pixels.index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned-2/pixels.index");
        config.addProperty("cache.location", "/mnt/nvme1n1/partitioned-2/pixels.cache");
        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(true)
                .setWriteContent(true)
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .setIndexType(indexType)
                .build2();
        // construct the layout and files
        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnletOrders
        Set<String> cacheColumnletOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        assert(writer.bulkLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0])) == 0);

        PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheLocation(cacheConfig.getCacheLocation())
                .setIndexLocation(cacheConfig.getIndexLocation()).setIndexType(indexType).build2();
        // search the key
        byte[] buf = new byte[40960];
        for (int index = 0; index < pixelsCacheIdxs.size(); ++index) {
            PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
            PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);

            // the offset is expected to be different. since this offset is based on the original cache, we can use
            // length as an indicator
            if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
            int readBytes = reader.get(cacheKey, buf, cacheIdx.length);
            if (readBytes == 0) {
                ByteBuffer keyBuf = ByteBuffer.allocate(4);
                keyBuf.putShort(cacheKey.rowGroupId);
                keyBuf.putShort(cacheKey.columnId);
                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                System.out.println("readBytes=0 " + partition + " " + index + " " + cacheKey + " " + cacheIdx);
            } else {
                byte ele = buf[0];
                for (int j = 0; j < readBytes; ++j) {
                    if (ele != buf[j])  {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("corrupted cache column chunk " + partition + " " + index + " " + cacheKey + " " + cacheIdx);
                        break;
                    }
                }
            }
        }

    }


    // incremental load will update the cache based on last free+start version, you can run it multiple times
    // by "static" we mean that only after incremental load, we use a reader to check the sanity of the data
    void testStaticIncrementalLoadIndex(String indexType) throws Exception {
        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(false) // dont overwrite
                .setWriteContent(false)
                .setIndexType(indexType)
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .build();
        // construct the layout and files
        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnletOrders
        Set<String> cacheColumnletOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
//        writer.bulkLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0]));
        assert(writer.incrementalLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0])) == 0);

        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexDiskFile = new MemoryMappedFile(cacheConfig.getIndexDiskLocation(), realIndexSize);
        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);

        PartitionCacheReader reader = PartitionCacheReader.newBuilder().setIndexType(indexType)
                .setCacheFile(cacheFile).setIndexFile(indexFile).build();

        // search the key
        for (int index = 0; index < pixelsCacheIdxs.size(); ++index) {
            PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
            PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);

            // the offset is expected to be different. since this offset is based on the original cache, we can use
            // length as an indicator
            PixelsCacheIdx readCacheIdx = reader.search(cacheKey);

            if (readCacheIdx != null) {
                assert (cacheIdx.length == readCacheIdx.length);

            } else {
                ByteBuffer keyBuf = ByteBuffer.allocate(4);
                keyBuf.putShort(cacheKey.rowGroupId);
                keyBuf.putShort(cacheKey.columnId);
                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                System.out.println(partition + " " + index + " " + cacheKey + " " + cacheIdx);

            }
        }

    }

    // by "dynamic", we mean that reader + writer at the same time
    void testDynamicIncrementalLoadIndex(String indexType) throws Exception {
        int nReaders = 32;
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once
        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexDiskFile = new MemoryMappedFile(cacheConfig.getIndexDiskLocation(), realIndexSize);
        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);

        SettableFuture<Integer> finish = SettableFuture.create();

        for (int i = 0; i < nReaders; ++i) {
            PartitionCacheReader reader = PartitionCacheReader.newBuilder().setIndexType(indexType).setCacheFile(cacheFile).setIndexFile(indexFile).build();
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            readExecutor.submit(() -> {
                Random random = new Random();
                int cnt = 0;
                while (!finish.isDone()) {
                    int index = random.nextInt(pixelsCacheKeys.size());

                    PixelsCacheIdx readed = reader.search(pixelsCacheKeys.get(index));
                    if (readed != null) {
                        assert (readed.length == pixelsCacheIdxs.get(index).length);
                        cnt++;

                    } else {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(pixelsCacheKeys.get(index).rowGroupId);
                        keyBuf.putShort(pixelsCacheKeys.get(index).columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println(partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    }
                }
                System.out.println("=================================================");
                System.out.println("read " + cnt + " keys");
                System.out.println("=================================================");

            });
        }

        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        PixelsPartitionCacheWriter writer = builder.setCacheLocation(cacheConfig.getCacheLocation())
                .setPartitions(cacheConfig.getPartitions())
                .setCacheSize(cacheConfig.getCacheSize())
                .setIndexLocation(cacheConfig.getIndexLocation())
                .setIndexSize(cacheConfig.getIndexSize())
                .setIndexDiskLocation(cacheConfig.getIndexDiskLocation())
                .setOverwrite(false) // dont overwrite
                .setWriteContent(false)
                .setIndexType(indexType)
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .build();

        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnletOrders
        Set<String> cacheColumnletOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        assert (writer.incrementalLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0])) == 0);

        finish.set(1);
    }

    @Test
    public void testBulkLoadRadixIndex() throws Exception {
        testBulkLoadIndex("radix");
    }

    @Test
    public void testBulkLoadRadixIndex2() throws Exception {
        ConfigFactory config = ConfigFactory.Instance();

        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache-2/pixels.index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned-2/pixels.index");
        config.addProperty("cache.location", "/mnt/nvme1n1/partitioned-2/pixels.cache");

        testBulkLoadIndex2("radix");
    }

    @Test
    public void testBulkLoadHashIndex() throws Exception {
        ConfigFactory config = ConfigFactory.Instance();
        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache/pixels.hash-index");
//        config.addProperty("index.disk.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.hash-index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned/pixels.hash-index");

        testBulkLoadIndex("hash");
    }

    @Test
    public void testBulkLoadHashIndex2() throws Exception {
        ConfigFactory config = ConfigFactory.Instance();
        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache-2/pixels.hash-index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned-2/pixels.hash-index");
        config.addProperty("cache.location", "/mnt/nvme1n1/partitioned-2/pixels.cache");


        testBulkLoadIndex2("hash");
    }

    @Test
    public void testBulkLoadRadixIndexAndContent() throws Exception {
        testBulkLoadIndexAndContent("radix");
    }

    @Test
    public void testBulkLoadHashIndexAndContent() throws Exception {
        ConfigFactory config = ConfigFactory.Instance();

        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache/pixels.hash-index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned/pixels.hash-index");
        config.addProperty("cache.location", "/mnt/nvme1n1/partitioned/pixels.cache");
        testBulkLoadIndexAndContent("hash");
    }

    @Test
    public void testBulkLoadRadixIndexAndContent2() throws Exception {
        testBulkLoadIndexAndContent2("radix");
    }

    @Test
    public void testBulkLoadHashIndexAndContent2() throws Exception {
        ConfigFactory config = ConfigFactory.Instance();

        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache-2/pixels.hash-index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned-2/pixels.hash-index");
        config.addProperty("cache.location", "/mnt/nvme1n1/partitioned-2/pixels.cache");
        testBulkLoadIndexAndContent2("hash");
    }

    @Test
    public void testStaticIncrementalLoadRadixIndex() throws Exception {
        testStaticIncrementalLoadIndex("radix");
    }

    @Test
    public void testStaticIncrementalLoadHashIndex() throws Exception {
        ConfigFactory config = ConfigFactory.Instance();
        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache/pixels.hash-index");
//        config.addProperty("index.disk.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.hash-index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned/pixels.hash-index");

        testStaticIncrementalLoadIndex("hash");
    }

    @Test
    public void testDynamicIncrementalLoadRadixIndex() throws Exception {
        testDynamicIncrementalLoadIndex("radix");
    }

    @Test
    public void testDynamicIncrementalLoadHashIndex() throws Exception {
        ConfigFactory config = ConfigFactory.Instance();
        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache/pixels.hash-index");
//        config.addProperty("index.disk.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.hash-index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned/pixels.hash-index");
        testDynamicIncrementalLoadIndex("hash");
    }

    @Test
    public void testDynamicIncrementalLoadIndexAndContent() throws Exception {
        int nReaders = 1;
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once
        long realIndexSize = cacheConfig.getIndexSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = cacheConfig.getCacheSize() / (cacheConfig.getPartitions()) * (cacheConfig.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexDiskFile = new MemoryMappedFile(cacheConfig.getIndexDiskLocation(), realIndexSize);
        MemoryMappedFile indexFile = new MemoryMappedFile(cacheConfig.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(cacheConfig.getCacheLocation(), realCacheSize);

        SettableFuture<Integer> finish = SettableFuture.create();

        for (int i = 0; i < nReaders; ++i) {
            PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheFile(cacheFile).setIndexFile(indexFile).build();
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            readExecutor.submit(() -> {
                Random random = new Random();
                int cnt = 0;
                byte[] buf = new byte[4096];
//                int index = 0;
                while (!finish.isDone()) {
                    int index = random.nextInt(pixelsCacheKeys.size());
//                    index = (index + 1) % 512000;
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
//
                    int readBytes = reader.get(cacheKey, buf, cacheIdx.length);
                    if (readBytes > 0) {
////                        byte ele = buf[0];
////                        for (int j = 0; j < cacheIdx.length; j++) {
////                            if (buf[j] != ele) {
////                                ByteBuffer keyBuf = ByteBuffer.allocate(4);
////                                keyBuf.putShort(cacheKey.rowGroupId);
////                                keyBuf.putShort(cacheKey.columnId);
////                                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
////                                System.out.println(partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
////                                break;
////                            }
////                        }
                    } else {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    }

                    cnt++;
                }
                System.out.println("=================================================");
                System.out.println("read " + cnt + " keys");
                System.out.println("=================================================");

            });
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
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .build();

        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnletOrders
        Set<String> cacheColumnletOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        assert (writer.incrementalLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0])) == 0);

        finish.set(1);
    }

    @Test
    public void testDynamicIncrementalLoadIndexAndContent2() throws Exception {
        ConfigFactory config = ConfigFactory.Instance();

        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache-2/pixels.index");
        config.addProperty("index.disk.location", "/mnt/nvme1n1/partitioned-2/pixels.index");
        config.addProperty("cache.location", "/mnt/nvme1n1/partitioned-2/pixels.cache");

        int nReaders = 1;
        String hostName = "diascld34";
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        // 1 reader continuously randomly read all keys
        // 1 writer write the partitions once

        SettableFuture<Integer> finish = SettableFuture.create();

        for (int i = 0; i < nReaders; ++i) {
//            PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheFile(cacheFile).setIndexFile(indexFile).build();
            PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheLocation(cacheConfig.getCacheLocation())
                    .setIndexLocation(cacheConfig.getIndexLocation()).build2();
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            readExecutor.submit(() -> {
                Random random = new Random();
                int cnt = 0;
                byte[] buf = new byte[4096];
//                int index = 0;
                while (!finish.isDone()) {
                    int index = random.nextInt(pixelsCacheKeys.size());
//                    index = (index + 1) % 512000;
                    PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
                    PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
                    if (buf.length < cacheIdx.length) buf = new byte[cacheIdx.length];
//
                    int readBytes = reader.get(cacheKey, buf, cacheIdx.length);
                    if (readBytes > 0) {
////                        byte ele = buf[0];
////                        for (int j = 0; j < cacheIdx.length; j++) {
////                            if (buf[j] != ele) {
////                                ByteBuffer keyBuf = ByteBuffer.allocate(4);
////                                keyBuf.putShort(cacheKey.rowGroupId);
////                                keyBuf.putShort(cacheKey.columnId);
////                                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
////                                System.out.println(partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
////                                break;
////                            }
////                        }
                    } else {
                        ByteBuffer keyBuf = ByteBuffer.allocate(4);
                        keyBuf.putShort(cacheKey.rowGroupId);
                        keyBuf.putShort(cacheKey.columnId);
                        int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % cacheConfig.getPartitions();
                        System.out.println("readBytes=0 " + partition + " " + index + " " + pixelsCacheKeys.get(index) + " " + pixelsCacheIdxs.get(index));
                    }

                    cnt++;
                }
                System.out.println("=================================================");
                System.out.println("read " + cnt + " keys");
                System.out.println("=================================================");

            });
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
                .setHostName(hostName)
                .setCacheConfig(cacheConfig)
                .build2();

        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnletOrders
        Set<String> cacheColumnletOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        assert (writer.incrementalLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0])) == 0);

        finish.set(1);
    }
}
