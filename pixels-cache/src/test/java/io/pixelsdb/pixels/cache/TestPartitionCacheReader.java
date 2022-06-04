package io.pixelsdb.pixels.cache;

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
import java.util.Collections;
import java.util.List;

public class TestPartitionCacheReader {

    List<PixelsCacheIdx> pixelsCacheIdxs = new ArrayList<>(4096);
    List<PixelsCacheKey> pixelsCacheKeys = new ArrayList<>(4096);

    @Before
    public void prepare() {
//        LogManager.getRootLogger().atDebug();
        Configurator.setRootLevel(Level.DEBUG);
    }

    @Before
    public void loadMockData() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("tmp.txt"));
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
        config.addProperty("cache.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.cache");
        config.addProperty("cache.size", String.valueOf(70 * 1024 * 1024 * 1024L)); // 70GiB
        config.addProperty("cache.partitions", "32");


        config.addProperty("index.location", "/dev/shm/pixels-partitioned-cache/pixels.index");
        config.addProperty("index.disk.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.index");
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
    public void readSimpleKey() throws Exception {
        int index = 10000;
        PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
        PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
        PixelsCacheConfig config = new PixelsCacheConfig();
        long realIndexSize = config.getIndexSize() * (config.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        MemoryMappedFile indexFile = new MemoryMappedFile(config.getIndexLocation(), realIndexSize);
        CacheIndexReader reader = new PartitionRadixIndexReader.Builder().setIndexFile(indexFile).build();
        System.out.println(cacheKey);
        // the offset is expected to be different. since this offset is based on the original cache, we can use
        // length as an indicator
        assert (cacheIdx.length == reader.read(cacheKey).length);
    }

    @Test
    public void readAllKeys() throws Exception {
        PixelsCacheConfig config = new PixelsCacheConfig();
        long realIndexSize = config.getIndexSize() / (config.getPartitions()) * (config.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
//        MemoryMappedFile indexFile = new MemoryMappedFile(config.getIndexDiskLocation(), realIndexSize);
        MemoryMappedFile indexFile = new MemoryMappedFile(config.getIndexLocation(), realIndexSize);

        CacheIndexReader reader = new PartitionRadixIndexReader.Builder().setIndexFile(indexFile).build();
        for (int index = 0; index < pixelsCacheIdxs.size(); ++index) {
            PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
            PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);

            // the offset is expected to be different. since this offset is based on the original cache, we can use
            // length as an indicator
            PixelsCacheIdx readCacheIdx = reader.read(cacheKey);
            if (readCacheIdx != null) {
                assert (cacheIdx.length == reader.read(cacheKey).length);
            } else {
                ByteBuffer keyBuf = ByteBuffer.allocate(4);
                keyBuf.putShort(cacheKey.rowGroupId);
                keyBuf.putShort(cacheKey.columnId);
                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % config.getPartitions();
                System.out.println(partition + " " + index + " " + cacheKey + " " + cacheIdx);

            }
        }
    }

    @Test
    public void readSimpleContent() throws Exception {
        int index = 400000;
        PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
        PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
        PixelsCacheConfig config = new PixelsCacheConfig();
        long realIndexSize = config.getIndexSize() / (config.getPartitions()) * (config.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = config.getCacheSize() / (config.getPartitions()) * (config.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

        MemoryMappedFile indexFile = new MemoryMappedFile(config.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(config.getCacheLocation(), realCacheSize);

        PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheFile(cacheFile).setIndexFile(indexFile).build();

        System.out.println(cacheKey);
        // the offset is expected to be different. since this offset is based on the original cache, we can use
        // length as an indicator
        ByteBuffer buf = reader.get(cacheKey);
        assert(buf != null);
        byte ele = buf.get(0);
        System.out.println(ele);
        for (int i = 1; i < cacheIdx.length; ++i) {
            assert(ele == buf.get(i));
        }
    }

    @Test
    public void readContent() throws Exception {
        PixelsCacheConfig config = new PixelsCacheConfig();
        long realIndexSize = config.getIndexSize() / (config.getPartitions()) * (config.getPartitions() + 1) + PixelsCacheUtil.PARTITION_INDEX_META_SIZE;
        long realCacheSize = config.getCacheSize() / (config.getPartitions()) * (config.getPartitions() + 1) + PixelsCacheUtil.CACHE_DATA_OFFSET;

//        MemoryMappedFile indexFile = new MemoryMappedFile(config.getIndexDiskLocation(), realIndexSize);
        MemoryMappedFile indexFile = new MemoryMappedFile(config.getIndexLocation(), realIndexSize);
        MemoryMappedFile cacheFile = new MemoryMappedFile(config.getCacheLocation(), realCacheSize);

        PartitionCacheReader reader = PartitionCacheReader.newBuilder().setCacheFile(cacheFile).setIndexFile(indexFile).build();

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
                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % config.getPartitions();
                System.out.println(partition + " " + index + " " + cacheKey + " " + cacheIdx);

            }
        }

        // get the value
//        for (int index = 0; index < pixelsCacheIdxs.size(); ++index) {
//            PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
//            PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);
//
//            // the offset is expected to be different. since this offset is based on the original cache, we can use
//            // length as an indicator
//            ByteBuffer buf = reader.get(cacheKey);
//            if (buf != null) {
////                byte ele = buf.get(0);
////                for (int i = 1; i < cacheIdx.length; ++i) assert(buf.get(i) == ele);
//
//            } else {
//                ByteBuffer keyBuf = ByteBuffer.allocate(4);
//                keyBuf.putShort(cacheKey.rowGroupId);
//                keyBuf.putShort(cacheKey.columnId);
//                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % config.getPartitions();
//                System.out.println(partition + " " + index + " " + cacheKey + " " + cacheIdx);
//            }
//        }

        byte[] readBuf = new byte[4096];
        int readCnt = pixelsCacheIdxs.size();
        System.out.println("readCnt=" + readCnt);

        for (int index = 0; index < readCnt; ++index) {
            PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(index);
            PixelsCacheKey cacheKey = pixelsCacheKeys.get(index);

            // the offset is expected to be different. since this offset is based on the original cache, we can use
            // length as an indicator
            if (readBuf.length < cacheIdx.length) readBuf = new byte[cacheIdx.length];
            int readBytes = reader.get(cacheKey, readBuf, cacheIdx.length);
            if (readBytes != 0) {
                byte ele = readBuf[0];
                for (int i = 1; i < readBytes; ++i) assert(readBuf[i] == ele);
//                byte ele = buf.get(0);
//                for (int i = 1; i < cacheIdx.length; ++i) assert(buf.get(i) == ele);

            } else {
                ByteBuffer keyBuf = ByteBuffer.allocate(4);
                keyBuf.putShort(cacheKey.rowGroupId);
                keyBuf.putShort(cacheKey.columnId);
                int partition = PixelsCacheUtil.hashcode(keyBuf.array()) & 0x7fffffff % config.getPartitions();
                System.out.println(partition + " " + index + " " + cacheKey + " " + cacheIdx);
            }

        }

    }
}