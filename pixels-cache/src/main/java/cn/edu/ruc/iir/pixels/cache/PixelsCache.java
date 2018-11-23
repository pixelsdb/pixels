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
    private final PixelsRadix radix;

    private PixelsCache(MemoryMappedFile cacheFile, MemoryMappedFile indexFile, int scheduledSeconds,
                        MappedBusReader mqReader)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.scheduledSeconds = scheduledSeconds;
        this.executorService = new ScheduledThreadPoolExecutor(1);
        this.mqReader = mqReader;
        this.radix = new PixelsRadix();
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
    }


}
