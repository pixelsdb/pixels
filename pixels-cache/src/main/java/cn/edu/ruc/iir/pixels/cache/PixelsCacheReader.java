package cn.edu.ruc.iir.pixels.cache;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels cache reader.
 *
 * @author guodong
 */
public class PixelsCacheReader
        implements AutoCloseable
{
    private final static int KEY_HEADER_SIZE = 2;
    private final static long CHILDREN_OFFSET_MASK = 0x00FFFFFFFFFFFFFFL;
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;

    private PixelsCacheReader(MemoryMappedFile cacheFile, MemoryMappedFile indexFile)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
    }

    public static class Builder
    {
        private String builderCacheLocation = "";
        private long builderCacheSize;
        private String builderIndexLocation = "";
        private long builderIndexSize;

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

        public PixelsCacheReader build() throws Exception
        {
            MemoryMappedFile cacheFile = new MemoryMappedFile(builderCacheLocation, builderCacheSize);
            MemoryMappedFile indexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);

            return new PixelsCacheReader(cacheFile, indexFile);
        }
    }

    public static PixelsCacheReader.Builder newBuilder()
    {
        return new PixelsCacheReader.Builder();
    }

    public int getVersion()
    {
        return PixelsCacheUtil.getIndexVersion(indexFile);
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
    public byte[] get(String blockId, short rowGroupId, short columnId)
    {
        byte[] content = new byte[0];
        // check rw flag
        short rwFlag = PixelsCacheUtil.getIndexRW(indexFile);
        if (rwFlag != 0) {
            return content;
        }

        // check if reader count reaches its max value (short max value)
        int readerCount = PixelsCacheUtil.getIndexReaderCount(indexFile);
        if (readerCount >= Short.MAX_VALUE) {
            return content;
        }
        // update reader count
        readerCount = readerCount + 1;
        PixelsCacheUtil.setIndexReaderCount(indexFile, (short) readerCount);

        // search index file for columnlet id
        ColumnletId columnletId = new ColumnletId(blockId, rowGroupId, columnId);
        byte[] cacheKeyBytes = columnletId.getBytes();

        // search cache key
        PixelsCacheIdx cacheIdx = search(cacheKeyBytes);
        // if found, read content from cache
        if (cacheIdx != null) {
            long offset = cacheIdx.getOffset();
            int length = cacheIdx.getLength();
            content = new byte[length];
            // increment counter
            cacheFile.getAndAddLong(offset, 1);
            // read content
            cacheFile.getBytes(offset + 4, content, 0, length);
        }
        // if not found, send cache miss message
//        else {
//            mqWriter.write(columnletId);
//        }

        // decrease reader count
        readerCount = indexFile.getShortVolatile(2);
        if (readerCount >= 1) {
            readerCount--;
        }
        indexFile.putShortVolatile(2, (short) readerCount);

        return content;
    }

    /**
     * Search key from radix tree.
     * If found, update counter in cache idx.
     * Else, return null
     * */
    private PixelsCacheIdx search(byte[] key)
    {
        long nodeOffset = 0;
        final int keyLen = key.length;
        int bytesMatched = 0;
        int childrenNum = 0;
        int edgeSize = 0;
        byte[] nodeHeader = new byte[2];
        byte[] edge;
        while (bytesMatched < keyLen) {
            boolean matched = false;
            nodeHeader = new byte[2];
            indexFile.getBytes(nodeOffset, nodeHeader, 0, 2);
            // get children num, if 0, return empty
            childrenNum = nodeHeader[1] + 128;
            edgeSize = nodeHeader[0] | 0x7F;
            edge = new byte[edgeSize];
            indexFile.getBytes(nodeOffset + 2 + childrenNum, edge, 0, edgeSize);

            // root node has node children, return null
            if (edgeSize == 0 && childrenNum == 0) {
                return null;
            }
            // search edge for matching
            int edgeIndex = 0;
            while (edgeIndex < edgeSize
                    && bytesMatched < keyLen
                    && key[bytesMatched] == edge[edgeIndex]) {
                edgeIndex++;
                bytesMatched++;
            }
            // if not matching current edge, then return null
            if (edgeIndex < edgeSize) {
                return null;
            }
            // if bytesMatched is equal to keyLen, then this is the node, then break
            if (bytesMatched == keyLen) {
                break;
            }
            // else search children further
            for (int i = 0; i < childrenNum; i++) {
                byte childLead = indexFile.getByte(nodeOffset + 2 + i * 8);
                // if found matching child, set this child as current node, and increment bytesMatched
                if (childLead == key[bytesMatched]) {
                    nodeOffset = indexFile.getLong(nodeOffset + 2 + i * 8);
                    nodeOffset = nodeOffset & CHILDREN_OFFSET_MASK;
                    bytesMatched++;
                    matched = true;
                    break;
                }
            }
            // if found no matching child, return null
            if (!matched) {
                return null;
            }
        }
        // found matching key, check if it has value
        if ((nodeHeader[0] >> 7 & 0x01) == 1) {
            // if it has value, get idx and increment counter
            long valueOffset = nodeOffset + KEY_HEADER_SIZE + childrenNum + edgeSize;
            long offset = indexFile.getLong(valueOffset);
            int length = indexFile.getInt(valueOffset + 8);
            return new PixelsCacheIdx(offset, length);
        }
        return null;
    }

    public void close() throws IOException
    {
//        mqWriter.close();
    }
}
