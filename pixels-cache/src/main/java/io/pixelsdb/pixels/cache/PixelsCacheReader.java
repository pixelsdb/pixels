package io.pixelsdb.pixels.cache;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * pixels cache reader.
 *
 * @author guodong
 */
public class PixelsCacheReader
        implements AutoCloseable
{
    //    private static final Logger logger = LogManager.getLogger(PixelsCacheReader.class);
//    private static CacheLogger cacheLogger = new CacheLogger();

    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;

    private byte[] children = new byte[256 * 8];
    private ByteBuffer childrenBuffer = ByteBuffer.wrap(children);
    private ByteBuffer keyBuffer = ByteBuffer.allocate(PixelsCacheKey.SIZE).order(ByteOrder.BIG_ENDIAN);

//    static
//    {
//        new Thread(cacheLogger).start();
//    }

    private PixelsCacheReader(MemoryMappedFile cacheFile, MemoryMappedFile indexFile)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
    }

    public static class Builder
    {
        private MemoryMappedFile builderCacheFile;
        private MemoryMappedFile builderIndexFile;

        private Builder()
        {
        }

        public PixelsCacheReader.Builder setCacheFile(MemoryMappedFile cacheFile)
        {
//            requireNonNull(cacheFile, "cache file is null");
            this.builderCacheFile = cacheFile;

            return this;
        }

        public PixelsCacheReader.Builder setIndexFile(MemoryMappedFile indexFile)
        {
//            requireNonNull(indexFile, "index file is null");
            this.builderIndexFile = indexFile;

            return this;
        }

        public PixelsCacheReader build()
        {
            return new PixelsCacheReader(builderCacheFile, builderIndexFile);
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
     * This method may return NULL value. Be careful dealing with null!!!
     *
     * @param blockId    block id
     * @param rowGroupId row group id
     * @param columnId   column id
     * @return columnlet content
     */
    public ByteBuffer get(long blockId, short rowGroupId, short columnId)
    {
        // search index file for columnlet id
        PixelsCacheKeyUtil.getBytes(keyBuffer, blockId, rowGroupId, columnId);

        ByteBuffer content = null;
        // search cache key
//        long searchBegin = System.nanoTime();
        PixelsCacheIdx cacheIdx = search(keyBuffer);
//        long searchEnd = System.nanoTime();
//        cacheLogger.addSearchLatency(searchEnd - searchBegin);
//        logger.debug("[cache search]: " + (searchEnd - searchBegin));
        // if found, read content from cache
//        long readBegin = System.nanoTime();
        if (cacheIdx != null)
        {
            content = ByteBuffer.allocate(cacheIdx.length);
            // read content
            cacheFile.getBytes(cacheIdx.offset, content.array(), 0, cacheIdx.length);
        }
//        long readEnd = System.nanoTime();
//        cacheLogger.addReadLatency(readEnd - readBegin);
//        logger.debug("[cache read]: " + (readEnd - readBegin));
        return content;
    }

    public void batchGet(List<ColumnletId> columnletIds, byte[][] container)
    {
        // TODO batch get cache items. merge cache accesses to reduce the number of jni invocation.
    }

    /**
     * This interface is only used by TESTS, DO NOT USE.
     * It will be removed soon!
     */
    public PixelsCacheIdx search(long blockId, short rowGroupId, short columnId)
    {
        PixelsCacheKeyUtil.getBytes(keyBuffer, blockId, rowGroupId, columnId);

        return search(keyBuffer);
    }

    /**
     * Search key from radix tree.
     * If found, update counter in cache idx.
     * Else, return null
     */
    private PixelsCacheIdx search(ByteBuffer keyBuffer)
    {
        int dramAccessCounter = 0;
        int radixLevel = 0;
        final int keyLen = keyBuffer.position();
        long currentNodeOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
        int bytesMatched = 0;
        int bytesMatchedInNodeFound = 0;

        // get root
        // TODO: does root node have an edge?
        int currentNodeHeader = indexFile.getInt(currentNodeOffset);
        dramAccessCounter++;
        int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
        int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
        if (currentNodeChildrenNum == 0 && currentNodeEdgeSize == 0)
        {
            return null;
        }
        radixLevel++;

        // search
        outer_loop:
        while (bytesMatched < keyLen)
        {
            radixLevel++;
            // search each child for the matching node
            long matchingChildOffset = 0L;
            indexFile.getBytes(currentNodeOffset + 4, children, 0, currentNodeChildrenNum * 8);
            dramAccessCounter++;
            childrenBuffer.position(0);
            childrenBuffer.limit(currentNodeChildrenNum * 8);
            for (int i = 0; i < currentNodeChildrenNum; i++)
            {
                long child = childrenBuffer.getLong();
                byte leader = (byte) ((child >>> 56) & 0xFF);
                if (leader == keyBuffer.get(bytesMatched))
                {
                    matchingChildOffset = child & 0x00FFFFFFFFFFFFFFL;
                    break;
                }
            }
            if (matchingChildOffset == 0)
            {
                break;
            }

            currentNodeOffset = matchingChildOffset;
            bytesMatchedInNodeFound = 0;
            currentNodeHeader = indexFile.getInt(currentNodeOffset);
            dramAccessCounter++;
            currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
            currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
            // TODO: does max length of edge = 12? can we move currentNodeEdge allocation out before this loop?
            byte[] currentNodeEdge = new byte[currentNodeEdgeSize];
            // TODO: can we get header, edge and children of a node in one memory access?
            indexFile.getBytes(currentNodeOffset + 4 + currentNodeChildrenNum * 8,
                    currentNodeEdge, 0, currentNodeEdgeSize);
            dramAccessCounter++;
            // TODO: numEdgeBytes seems redundant.
            for (int i = 0, numEdgeBytes = currentNodeEdgeSize; i < numEdgeBytes && bytesMatched < keyLen; i++)
            {
                if (currentNodeEdge[i] != keyBuffer.get(bytesMatched))
                {
                    break outer_loop;
                }
                bytesMatched++;
                bytesMatchedInNodeFound++;
            }
        }

        // if matches, node found
        if (bytesMatched == keyLen && bytesMatchedInNodeFound == currentNodeEdgeSize)
        {
            if (((currentNodeHeader >>> 31) & 1) > 0)
            {
                byte[] idx = new byte[12];
                indexFile.getBytes(currentNodeOffset + 4 + (currentNodeChildrenNum * 8) + currentNodeEdgeSize,
                        idx, 0, 12);
                dramAccessCounter++;
                PixelsCacheIdx cacheIdx = new PixelsCacheIdx(idx);
                cacheIdx.dramAccessCount = dramAccessCounter;
                cacheIdx.radixLevel = radixLevel;
                return cacheIdx;
            }
        }
        return null;
    }

    public void close()
    {
        try
        {
//            logger.info("cache reader unmaps cache/index file");
            cacheFile.unmap();
            indexFile.unmap();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
