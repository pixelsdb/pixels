package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.cache.mq.MappedBusWriter;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels cache reader.
 *
 * @author guodong
 */
public class PixelsCacheReader
{
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final Map<ColumnletId, ColumnletIdx> index;
    private final MappedBusWriter mqWriter;
    private int indexVersion = 0;

    private PixelsCacheReader(MemoryMappedFile cacheFile, MemoryMappedFile indexFile, MappedBusWriter mqWriter)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.index = new HashMap<>();
        this.mqWriter = mqWriter;
    }

    public static class Builder
    {
        private String builderCacheLocation = "";
        private long builderCacheSize;
        private String builderIndexLocation = "";
        private long builderIndexSize;
        private String builderMQLocation = "";
        private long builderMQFileSize;
        private int builderMQRecordSize;
        private boolean builderMQAppend;

        private Builder()
        {}

        public PixelsCacheReader.Builder setCacheLocation(String cacheLocation)
        {
            checkArgument(!cacheLocation.isEmpty(), "location should not be empty");
            this.builderCacheLocation = cacheLocation;

            return this;
        }

        public PixelsCacheReader.Builder setCacheSize(long cacheSize)
        {
            checkArgument(cacheSize > 0, "size should be positive");
            this.builderCacheSize = cacheSize;

            return this;
        }

        public PixelsCacheReader.Builder setIndexLocation(String location)
        {
            checkArgument(!location.isEmpty(), "index location should not be empty");
            this.builderIndexLocation = location;

            return this;
        }

        public PixelsCacheReader.Builder setIndexSize(long size)
        {
            checkArgument(size > 0, "index size should be positive");
            this.builderIndexSize = size;

            return this;
        }

        public PixelsCacheReader.Builder setMQLocation(String mqLocation)
        {
            checkArgument(!mqLocation.isEmpty(), "location should not be empty");
            this.builderMQLocation = mqLocation;

            return this;
        }

        public PixelsCacheReader.Builder setMQFileSize(long mqFileSize)
        {
            checkArgument(mqFileSize > 0, "message queue file size should be positive");
            this.builderMQFileSize = mqFileSize;

            return this;
        }

        public PixelsCacheReader.Builder setMQRecordSize(int mqRecordSize)
        {
            checkArgument(mqRecordSize > 0, "message queue record size should be positive");
            this.builderMQRecordSize = mqRecordSize;

            return this;
        }

        public PixelsCacheReader.Builder setMQAppend(boolean mqAppend)
        {
            this.builderMQAppend = mqAppend;

            return this;
        }

        public PixelsCacheReader build() throws Exception
        {
            MappedBusWriter mqWriter = new MappedBusWriter(builderMQLocation, builderMQFileSize,
                    builderMQRecordSize, builderMQAppend);
            mqWriter.open();
            MemoryMappedFile cacheFile = new MemoryMappedFile(builderCacheLocation, builderCacheSize);
            MemoryMappedFile indexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);

            return new PixelsCacheReader(cacheFile, indexFile, mqWriter);
        }
    }

    public static PixelsCacheReader.Builder newBuilder()
    {
        return new PixelsCacheReader.Builder();
    }

    /**
     * Read specified columnlet from cache.
     * If cache is not hit, empty byte array is returned, and an access message is sent to the mq.
     * If cache is hit, columnlet content is returned as byte array.
     * @param blockId block id
     * @param rowGroupId row group id
     * @param columnId column id
     * @return columnlet content
     * */
    public byte[] get(long blockId, int rowGroupId, int columnId) throws EOFException
    {
        byte[] content = new byte[0];
        ColumnletId columnletId = new ColumnletId(blockId, rowGroupId, columnId);
        int header = indexFile.getIntVolatile(0);
        // check rw flag
        if (PixelsCacheUtil.getHeaderRW(header)) {
            return content;
        }
        // increase reader count
        int newHeader = PixelsCacheUtil.incrementReadCount(header);
        while (!indexFile.compareAndSwapInt(0, header, newHeader)) {
            header = indexFile.getIntVolatile(0);
            newHeader = PixelsCacheUtil.incrementReadCount(header);
        }
        int version = indexFile.getInt(32);
        if (version > indexVersion) {
            updateIndex();
            indexVersion = version;
        }
        if (index.containsKey(columnletId)) {
            ColumnletIdx idx = index.get(columnletId);
            // read content
            long contentOffset = idx.getOffset();
            int contentLength = idx.getLength();
            content = new byte[contentLength];
            cacheFile.getBytes(contentOffset, content, 0, contentLength);
        }

        ByteBuffer idBuf = ByteBuffer.allocate(16);
        idBuf.putLong(blockId);
        idBuf.putInt(rowGroupId);
        idBuf.putInt(columnId);
        idBuf.flip();
        mqWriter.write(idBuf.array(), 0, 16);

        // decrease reader count
        newHeader = PixelsCacheUtil.decrementReadCount(header);
        while (!indexFile.compareAndSwapInt(0, header, newHeader)) {
            header = indexFile.getIntVolatile(0);
            newHeader = PixelsCacheUtil.decrementReadCount(header);
        }

        return content;
    }

    private void updateIndex()
    {
        long indexSize = indexFile.getLongVolatile(128);
        int offset = PixelsCacheUtil.INDEX_FIELD_OFFSET;
        while (indexSize > 0) {
            // get key
            long blockId = indexFile.getLongVolatile(offset);
            offset += Long.BYTES;
            long keyright = indexFile.getLongVolatile(offset);
            int rowGroupId = (int)(keyright >> 32);
            int columndId = (int)(keyright & 0x00000000FFFFFFFFL);
            offset += Long.BYTES;

            // get value
            long vOffset = indexFile.getLongVolatile(offset);
            offset += Long.BYTES;
            long valueright = indexFile.getLongVolatile(offset);
            offset += Long.BYTES;
            int length = (int)(valueright >> 32);
            int count = (int)(valueright & 0x00000000FFFFFFFFL);

            ColumnletId columnletId = new ColumnletId(blockId, rowGroupId, columndId);
            ColumnletIdx columnletIdx = new ColumnletIdx(offset, length, count);
            index.put(columnletId, columnletIdx);
        }
    }

    public void pin(long blockId, int rowGroupId, int columnId)
    {}

    public void unPin(long blockId, int rowGroupId, int columnId)
    {}
}
