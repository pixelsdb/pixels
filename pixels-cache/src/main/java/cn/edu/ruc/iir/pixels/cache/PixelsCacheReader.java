package cn.edu.ruc.iir.pixels.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.util.Objects.requireNonNull;

/**
 * pixels cache reader.
 *
 * @author guodong
 */
public class PixelsCacheReader
        implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(PixelsCacheReader.class);

    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
//    private final ReentrantLock lock;

    private PixelsCacheReader(MemoryMappedFile cacheFile, MemoryMappedFile indexFile)
    {
        logger.info("Pixels cache reader is initialized");
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
//        this.lock = new ReentrantLock();
    }

    public static class Builder
    {
        private MemoryMappedFile builderCacheFile;
        private MemoryMappedFile builderIndexFile;

        private Builder()
        {}

        public PixelsCacheReader.Builder setCacheFile(MemoryMappedFile cacheFile)
        {
            requireNonNull(cacheFile, "cache file is null");
            this.builderCacheFile = cacheFile;

            return this;
        }

        public PixelsCacheReader.Builder setIndexFile(MemoryMappedFile indexFile)
        {
            requireNonNull(indexFile, "index file is null");
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
     * @param blockId block id
     * @param rowGroupId row group id
     * @param columnId column id
     * @return columnlet content
     * */
    public byte[] get(String blockId, short rowGroupId, short columnId)
    {
//        lock.lock();
//        logger.debug("Cache access: " + blockId + "-" + rowGroupId + "-" + columnId);
        byte[] content = new byte[0];
        String cacheGetId = blockId + "-" + rowGroupId + "-" + columnId;
        // check rw flag, if not readable, return empty bytes
//        short rwFlag = PixelsCacheUtil.getIndexRW(indexFile);
//        if (rwFlag != PixelsCacheUtil.RWFlag.READ.getId()) {
//            logger.debug("Index rwFlag is not set as READ. Stop.");
//            return content;
//        }

        // check if reader count reaches its max value, if so no more reads are allowed
//        int readerCount = PixelsCacheUtil.getIndexReaderCount(indexFile);
//        if (readerCount >= PixelsCacheUtil.MAX_READER_COUNT) {
//            logger.debug("Index reader count has exceeded the maximum value. Stop.");
//            return content;
//        }
        // update reader count
//        PixelsCacheUtil.indexReaderCountIncrement(indexFile);

        // search index file for columnlet id
        PixelsCacheKey cacheKey = new PixelsCacheKey(blockId, rowGroupId, columnId);
        byte[] cacheKeyBytes = cacheKey.getBytes();

        // search cache key
        long searchBeginNano = System.nanoTime();
        PixelsCacheIdx cacheIdx = search(cacheKeyBytes);
        long searchEndNano = System.nanoTime();
        logger.debug("[cache search]" + cacheGetId + "," + (searchEndNano - searchBeginNano));
        // if found, read content from cache
        if (cacheIdx != null) {
            long readBeginNano = System.nanoTime();
            long offset = cacheIdx.getOffset();
            int length = cacheIdx.getLength();
//            logger.debug("Cache entry(" + offset + "," + length + ") is found for " + blockId + "-" + rowGroupId + "-" + columnId);
            content = new byte[length];
            // read content
            cacheFile.getBytes(offset, content, 0, length);
            long readEndNano = System.nanoTime();
            logger.debug("[cache read]" + cacheGetId + "," + (readEndNano - readBeginNano));
        }
//        lock.unlock();

        // decrease reader count
//        PixelsCacheUtil.indexReaderCountDecrement(indexFile);

        return content;
    }

    /**
     * Search key from radix tree.
     * If found, update counter in cache idx.
     * Else, return null
     * */
    private PixelsCacheIdx search(byte[] key)
    {
        final int keyLen = key.length;
        long currentNodeOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
        int bytesMatched = 0;
        int bytesMatchedInNodeFound = 0;

        // get root
        int currentNodeHeader = indexFile.getInt(currentNodeOffset);
        int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
        int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
        if (currentNodeChildrenNum == 0 && currentNodeEdgeSize == 0) {
            return null;
        }

        // search
        outer_loop: while (bytesMatched < keyLen) {
            // search each child for the matching node
            long matchingChildOffset = 0L;
            for (int i = 0; i < currentNodeChildrenNum; i++) {
                long child = indexFile.getLong(currentNodeOffset + 4 + (8 * i));
                byte leader = (byte) ((child >>> 56) & 0xFF);
                if (leader == key[bytesMatched]) {
                    matchingChildOffset = child & 0x00FFFFFFFFFFFFFFL;
                    break;
                }
            }
            if (matchingChildOffset == 0) {
                break;
            }

            currentNodeOffset = matchingChildOffset;
            bytesMatchedInNodeFound = 0;
            currentNodeHeader = indexFile.getInt(currentNodeOffset);
            currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
            currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
            byte[] currentNodeEdge = new byte[currentNodeEdgeSize];
            indexFile.getBytes(currentNodeOffset + 4 + currentNodeChildrenNum * 8,
                               currentNodeEdge, 0, currentNodeEdgeSize);
            for (int i = 0, numEdgeBytes = currentNodeEdgeSize; i < numEdgeBytes && bytesMatched < keyLen; i++)
            {
                if (currentNodeEdge[i] != key[bytesMatched]) {
                    break outer_loop;
                }
                bytesMatched++;
                bytesMatchedInNodeFound++;
            }
        }

        // if matches, node found
        if (bytesMatched == keyLen && bytesMatchedInNodeFound == currentNodeEdgeSize) {
            if (((currentNodeHeader >>> 31) & 1) > 0) {
                byte[] idx = new byte[12];
                indexFile.getBytes(currentNodeOffset + 4 + (currentNodeChildrenNum * 8) + currentNodeEdgeSize,
                                         idx, 0, 12);
                return new PixelsCacheIdx(idx);
            }
        }
        return null;
    }

    public void close()
    {
        try {
            logger.info("cache reader close and unmap");
            cacheFile.unmap();
            indexFile.unmap();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
