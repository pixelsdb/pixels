package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.cache.mq.MappedBusReader;

import java.nio.ByteBuffer;
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
    private long currentIndexOffset = 0L;

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
        PixelsCacheManager cacheManager = new PixelsCacheManager(
                cacheFile, indexFile, mqReader, radix);
//        executorService.schedule(new PixelsCacheManager(cacheFile, indexFile, mqReader),
//                scheduledSeconds, TimeUnit.SECONDS);
    }

    private void writeRadix(RadixNode node)
    {
        flushNode(node);
        for (RadixNode n : node.getChildren().values()) {
            writeRadix(n);
        }
    }

    /**
     * Flush node content to the index file based on {@code currentIndexOffset}
     * Header(2 bytes) + [Child(1 byte)]{n} + edge(variable size) + value(optional)
     * */
    private void flushNode(RadixNode node)
    {
        node.offset = currentIndexOffset;
        currentIndexOffset += node.getLengthInBytes();
        ByteBuffer nodeBuffer = ByteBuffer.allocate(node.getLengthInBytes());
        int header = 0;
        int isKeyMask = 0x0001 << 15;
        if (node.isKey()) {
            header = header | isKeyMask;
        }
        int edgeSize = node.getEdge().length;
        header = header | (edgeSize << 7);
        header = header | node.getChildren().size();
        nodeBuffer.putShort((short) header);  // header
        for (RadixNode n : node.getChildren().values()) {   // children
            int len = n.getLengthInBytes();
            n.offset = currentIndexOffset;
            currentIndexOffset += len;
            long childId = 0L;
            long leader = n.getEdge()[0];  // 1 byte
            childId = childId & (leader << 56);  // leader
            childId = childId | n.offset;  // offset
            nodeBuffer.putLong(childId);
        }
        nodeBuffer.put(node.getEdge()); // edge
        if (node.isKey()) {  // value
            nodeBuffer.put(node.getValue().getBytes());
        }
        // flush bytes
        indexFile.putBytes(node.offset, nodeBuffer.array());
    }

    /**
     * Flush out index content
     * */
    private void flushIndex()
    {
        // 1. change rwFlag to 1
        indexFile.putShortVolatile(0, (short) 1);
        // 2. increase version number
        int version = indexFile.getInt(4) + 1;
        indexFile.putIntVolatile(4, version);
        // 3. traverse radix tree;
        if (radix.getRoot().getSize() != 0) {
            writeRadix(radix.getRoot());
        }
    }
}
