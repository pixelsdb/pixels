package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.cache.mq.MappedBusReader;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels cache
 *
 * @author guodong
 */
public class PixelsCache
{
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final int scheduledSeconds;
    private final ScheduledExecutorService executorService;
    private final MappedBusReader mqReader;

    private PixelsCache(MemoryMappedFile cacheFile, MemoryMappedFile indexFile, int scheduledSeconds,
                        MappedBusReader mqReader)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.scheduledSeconds = scheduledSeconds;
        this.executorService = new ScheduledThreadPoolExecutor(1);
        this.mqReader = mqReader;
    }

    public static class Builder
    {
        private String builderCacheLocation = "";
        private long builderCacheSize;
        private String builderIndexLocation = "";
        private long builderIndexSize;
        private int builderScheduledSeconds;
        private String builderMQLocation = "";
        private long builderMQSize;
        private int builderMQRecordSize;

        private Builder()
        {}

        public Builder setCacheLocation(String location)
        {
            checkArgument(!location.isEmpty(), "cache location should not be empty");
            this.builderCacheLocation = location;

            return this;
        }

        public Builder setCacheSize(long size)
        {
            checkArgument(size > 0, "cache size should be positive");
            this.builderCacheSize = size;

            return this;
        }

        public Builder setIndexLocation(String location)
        {
            checkArgument(!location.isEmpty(), "index location should not be empty");
            this.builderIndexLocation = location;

            return this;
        }

        public Builder setIndexSize(long size)
        {
            checkArgument(size > 0, "index size should be positive");
            this.builderIndexSize = size;

            return this;
        }

        public Builder setScheduleSeconds(int seconds)
        {
            checkArgument(seconds > 0, "scheduled seconds should be positive");
            this.builderScheduledSeconds = seconds;

            return this;
        }

        public Builder setMQLocation(String mqLocation)
        {
            checkArgument(!mqLocation.isEmpty(), "message queue location should not be empty");
            this.builderMQLocation = mqLocation;

            return this;
        }

        public Builder setMQSize(long mqSize)
        {
            checkArgument(mqSize > 0, "message queue size should be positive");
            this.builderMQSize = mqSize;

            return this;
        }

        public Builder setMQRecordSize(int mqRecordSize)
        {
            checkArgument(mqRecordSize > 0, "message queue record size should be positive");
            this.builderMQRecordSize = mqRecordSize;

            return this;
        }

        public PixelsCache build() throws Exception
        {
            MemoryMappedFile cacheFile = new MemoryMappedFile(builderCacheLocation, builderCacheSize);
            MemoryMappedFile indexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);
            MappedBusReader mqReader = new MappedBusReader(builderMQLocation, builderMQSize, builderMQRecordSize);
            mqReader.open();

            return new PixelsCache(cacheFile, indexFile, builderScheduledSeconds, mqReader);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    /**
     * Start cache service.
     * 1. Initialize cache
     * 2. Start scheduled manager
     * */
    public void start()
    {
        initialize();
//        executorService.schedule(new PixelsCacheManager(cacheFile, indexFile, mqReader),
//                scheduledSeconds, TimeUnit.SECONDS);
        while (true) {
        }
    }

    /**
     * Initialize cache.
     * 1. Check if index already exists
     * 2. If not exists, create a index file with version 1.
     * */
    private void initialize()
    {
        int version = indexFile.getInt(32);
        if (version == 0) {
            indexFile.putInt(32, 1);
            indexFile.putLong(64, 0);
            indexFile.putLong(128, 0);
        }
    }

    /**
     * Put specified columnlet into cache.
     * This operation has very high priority, it will trigger eviction if not enough space left.
     * @param blockId block id
     * @param rowGroupId row group id
     * @param columnId column id
     * @param content columnlet content
     * */
    public void put(long blockId, int rowGroupId, int columnId, byte[] content)
    {
        long cacheOffset = indexFile.getLongVolatile(64);
        long indexOffset = indexFile.getLongVolatile(128) + 192;
//        long timestamp = System.currentTimeMillis();
        int length = content.length;
        int count = 0;

        cacheFile.setBytes(cacheOffset, content, 0, length);
        indexFile.putLong(indexOffset, blockId);
        indexOffset += Long.BYTES;
        long keyright = (long)rowGroupId << 32 | columnId;
        indexFile.putLong(indexOffset, keyright);
        indexOffset += Long.BYTES;
        indexFile.putLong(indexOffset, cacheOffset);
//        indexOffset += Long.BYTES;
//        indexFile.putLong(indexOffset, timestamp);
        indexOffset += Long.BYTES;
        long valueright;
//        if (pin) {
//            valueright = (long)length << 32 | count | 0x01;
//        }
//        else {
//            valueright = ((long)length << 32 | count) & 0xFFFFFFFFFFFFF0L;
//        }
        valueright = (long) length << 32 | count;
        indexFile.putLong(indexOffset, valueright);
        indexFile.putLong(128, indexOffset);
    }
}
