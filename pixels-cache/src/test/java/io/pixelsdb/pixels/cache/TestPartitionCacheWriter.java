package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
        config.addProperty("cache.partitions", "16");


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
    public void testBulkLoad() throws Exception {
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
        // construct the layout and files
        // build files
        Set<String> files = pixelsCacheKeys.stream().map(key -> String.valueOf(key.blockId)).collect(Collectors.toSet());
        // build cacheColumnletOrders
        Set<String> cacheColumnletOrders = pixelsCacheKeys.stream().map(key -> key.rowGroupId + ":" + key.columnId).collect(Collectors.toSet());
        writer.bulkLoad(623, new ArrayList<>(cacheColumnletOrders), files.toArray(new String[0]));



    }
}
