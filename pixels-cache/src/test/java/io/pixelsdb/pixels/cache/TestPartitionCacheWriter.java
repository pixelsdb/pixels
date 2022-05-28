package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.junit.Test;

public class TestPartitionCacheWriter {
    @Test
    public void testBuild() throws Exception {

        PixelsPartitionCacheWriter.Builder builder = PixelsPartitionCacheWriter.newBuilder();
        ConfigFactory config = ConfigFactory.Instance();
        String hostName = "diascld34";
        // disk cache
        config.addProperty("cache.location", "/scratch/yeeef/pixels-cache/partitioned/pixels.cache");
        config.addProperty("cache.size", String.valueOf(70 * 1024 * 1024 * 1024L)); // 70GiB
        config.addProperty("cache.partitions", "4");


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
}
