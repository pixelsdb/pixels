package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import com.coreos.jetcd.data.KeyValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheWriter
{
    private final static short READABLE = 0;
    private final static Logger logger = LogManager.getLogger(PixelsCacheWriter.class);

    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final FileSystem fs;
    private final PixelsRadix radix;
    private final EtcdUtil etcdUtil;
    private final String host;
    private long currentIndexOffset;
    private long allocatedIndexOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
    private long cacheOffset = 0L;  // this is used in the write() method.

    private PixelsCacheWriter(MemoryMappedFile cacheFile,
                              MemoryMappedFile indexFile,
                              FileSystem fs,
                              PixelsRadix radix,
                              EtcdUtil etcdUtil,
                              String host)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.fs = fs;
        this.radix = radix;
        this.etcdUtil = etcdUtil;
        this.host = host;
    }

    public static class Builder
    {
        private String builderCacheLocation = "";
        private long builderCacheSize;
        private String builderIndexLocation = "";
        private long builderIndexSize;
        private FileSystem builderFS;
        private boolean builderOverwrite = true;
        private String builderHostName = null;

        private Builder()
        {}

        public PixelsCacheWriter.Builder setCacheLocation(String cacheLocation)
        {
            checkArgument(!cacheLocation.isEmpty(), "location should bot be empty");
            this.builderCacheLocation = cacheLocation;

            return this;
        }

        public PixelsCacheWriter.Builder setCacheSize(long cacheSize)
        {
            checkArgument(cacheSize > 0, "size should be positive");
            this.builderCacheSize = cacheSize;

            return this;
        }

        public PixelsCacheWriter.Builder setIndexLocation(String location)
        {
            checkArgument(!location.isEmpty(), "index location should not be empty");
            this.builderIndexLocation = location;

            return this;
        }

        public PixelsCacheWriter.Builder setIndexSize(long size)
        {
            checkArgument(size > 0, "index size should be positive");
            this.builderIndexSize = size;

            return this;
        }

        public PixelsCacheWriter.Builder setFS(FileSystem fs)
        {
            checkArgument(fs != null, "fs should not be null");
            this.builderFS = fs;

            return this;
        }

        public PixelsCacheWriter.Builder setOverwrite(boolean overwrite)
        {
            this.builderOverwrite = overwrite;
            return this;
        }

        public PixelsCacheWriter.Builder setHostName(String hostName)
        {
            this.builderHostName = hostName;
            return this;
        }

        public PixelsCacheWriter build()
                throws Exception
        {
            MemoryMappedFile cacheFile = new MemoryMappedFile(builderCacheLocation, builderCacheSize);
            MemoryMappedFile indexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);
            PixelsRadix radix;
            // check if cache and index exists.
            //   if overwrite is not true, and cache and index file already exists, reconstruct radix from existing index.
            if (!builderOverwrite && PixelsCacheUtil.checkMagic(indexFile) && PixelsCacheUtil.checkMagic(cacheFile)) {
                radix = PixelsCacheUtil.getIndexRadix(indexFile);
            }
            //   else, create a new radix tree, and initialize the index and cache file.
            else {
                radix = new PixelsRadix();
                PixelsCacheUtil.initialize(indexFile, cacheFile);
            }
            // todo check nulls
            EtcdUtil etcdUtil = EtcdUtil.Instance();

            return new PixelsCacheWriter(cacheFile, indexFile, builderFS, radix, etcdUtil, builderHostName);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public MemoryMappedFile getIndexFile()
    {
        return indexFile;
    }

    public boolean updateAll(int version, Layout layout)
    {
        try {
            // get the caching file list
            String key = Constants.CACHE_LOCATION_LITERAL + version + "_" + host;
            KeyValue keyValue = etcdUtil.getKeyValue(key);
            if (keyValue == null) {
                logger.debug("Found no allocated files. No updates are needed. " + key);
                return false;
            }
            String fileStr =  keyValue.getValue().toStringUtf8();
            String[] files = fileStr.split(";"); // todo split is inefficient
            internalUpdate(version, layout, files);
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Currently, this is an interface for unit tests.
     * This method only updates index content and cache content (without touching headers)
     * */
    public void write(PixelsCacheKey key, byte[] value)
    {
        PixelsCacheIdx cacheIdx = new PixelsCacheIdx(cacheOffset, value.length);
        cacheFile.putBytes(cacheOffset, value);
        cacheOffset += value.length;
        radix.put(key, cacheIdx);
    }

    /**
     * Currently, this is an interface for unit tests.
     * */
    public void flush()
    {
        flushIndex();
    }

    private void internalUpdate(int version, Layout layout, String[] files)
            throws IOException
    {
        // get the new caching layout
        Compact compact = layout.getCompactObject();
        int cacheBorder = compact.getCacheBorder();
        List<String> cacheColumnletOrders = compact.getColumnletOrder().subList(0, cacheBorder);
        // set rwFlag as write
        logger.debug("Set index rwFlag as write");
        PixelsCacheUtil.setIndexRW(indexFile, PixelsCacheUtil.RWFlag.WRITE.getId());
        // wait until readerCount is 0
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5000) {
            logger.debug("Wait until all previous started reads are finished");
            if (PixelsCacheUtil.getIndexReaderCount(indexFile) == 0) {
                logger.debug("No more reads are going on. Break.");
                break;
            }
        }
        PixelsCacheUtil.setIndexReaderCount(indexFile, (short) 0);
        logger.debug("Set index reader count to 0");
        // update cache content
        radix.removeAll();
        long cacheOffset = 0L;
        outer_loop: for (String file : files)
        {
            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(fs, new Path(file));
            int[] physicalLens = new int[cacheColumnletOrders.size()];
            long[] physicalOffsets = new long[cacheColumnletOrders.size()];
            // update radix and cache content
            for (int i = 0; i < cacheColumnletOrders.size(); i++)
            {
                String[] columnletIdStr = cacheColumnletOrders.get(i).split(":");
                short rowGroupId = Short.parseShort(columnletIdStr[0]);
                short columnId = Short.parseShort(columnletIdStr[1]);
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                PixelsProto.ColumnChunkIndex chunkIndex =
                        rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                physicalLens[i] = (int) chunkIndex.getChunkLength();
                physicalOffsets[i] = chunkIndex.getChunkOffset();
                if (cacheOffset + physicalLens[i] >= cacheFile.getSize()) {
                    logger.debug("Cache writes have exceeded cache size. Break. Current size: " + cacheOffset);
                    break outer_loop;
                }
                else {
                    logger.debug("Cache write: " + file + "-" + rowGroupId + "-" + columnId);
                    radix.put(new PixelsCacheKey(file, rowGroupId, columnId),
                              new PixelsCacheIdx(cacheOffset, physicalLens[i]));
                    byte[] columnlet = pixelsPhysicalReader.read(physicalOffsets[i], physicalLens[i]);
                    cacheFile.putBytes(cacheOffset, columnlet);
                    cacheOffset += physicalLens[i];
                }
            }
        }
        logger.debug("Cache writer ends at offset: " + cacheOffset);
        // update cache version
        PixelsCacheUtil.setIndexVersion(indexFile, version);
        // flush index
        flushIndex();
        logger.debug("Cache index ends at offset: " + currentIndexOffset);
        // set rwFlag as readable
        PixelsCacheUtil.setIndexRW(indexFile, READABLE);
    }

    /**
     * Traverse radix to get all cached values, and put them into cacheColumnlets list.
     * */
    private void traverseRadix(List<ColumnletId> cacheColumnlets)
    {
        RadixNode root = radix.getRoot();
        if (root.getSize() == 0) {
            return;
        }
        visitRadix(cacheColumnlets, root);
    }

    /**
     * Visit radix recursively in depth first way.
     * Maybe considering using a stack to store edge values along the visitation path.
     * Push edges in as going deeper, and pop out as going shallower.
     * */
    private void visitRadix(List<ColumnletId> cacheColumnlets, RadixNode node)
    {
        if (node.isKey()) {
            PixelsCacheIdx value = node.getValue();
            ColumnletId columnletId = new ColumnletId();
            columnletId.cacheOffset = value.getOffset();
            columnletId.cacheLength = value.getLength();
            cacheColumnlets.add(columnletId);
        }
        for (RadixNode n : node.getChildren().values()) {
            visitRadix(cacheColumnlets, n);
        }
    }

    /**
     * Write radix tree node.
     * */
    private void writeRadix(RadixNode node)
    {
        if (flushNode(node)) {
            for (RadixNode n : node.getChildren().values()) {
                writeRadix(n);
            }
        }
    }

    /**
     * Flush node content to the index file based on {@code currentIndexOffset}.
     * Header(4 bytes) + [Child(8 bytes)]{n} + edge(variable size) + value(optional).
     * Header: isKey(1 bit) + edgeSize(22 bits) + childrenSize(9 bits)
     * Child: leader(1 byte) + child_offset(7 bytes)
     * */
    // todo add index file size limitation
    private boolean flushNode(RadixNode node)
    {
        if (currentIndexOffset >= indexFile.getSize()) {
            logger.debug("Index file have exceeded cache size. Break. Current size: " + currentIndexOffset);
            return false;
        }
        if (node.offset == 0) {
            node.offset = currentIndexOffset;
        }
        else {
            currentIndexOffset = node.offset;
        }
        allocatedIndexOffset += node.getLengthInBytes();
        int header = 0;
        int edgeSize = node.getEdge().length;
        header = header | (edgeSize << 9);
        int isKeyMask = 1 << 31;
        if (node.isKey()) {
            header = header | isKeyMask;
        }
        header = header | node.getChildren().size();
        indexFile.putInt(currentIndexOffset, header);  // header
        currentIndexOffset += 4;
        for (Byte key : node.getChildren().keySet()) {   // children
            RadixNode n = node.getChild(key);
            int len = n.getLengthInBytes();
            n.offset = allocatedIndexOffset;
            allocatedIndexOffset += len;
            long childId = 0L;
            childId = childId | ((long) key << 56);  // leader
            childId = childId | n.offset;  // offset
            indexFile.putLong(currentIndexOffset, childId);
            currentIndexOffset += 8;
        }
        indexFile.putBytes(currentIndexOffset, node.getEdge()); // edge
        currentIndexOffset += node.getEdge().length;
        if (node.isKey()) {  // value
            indexFile.putBytes(currentIndexOffset, node.getValue().getBytes());
            currentIndexOffset += 12;
        }
        return true;
    }

    /**
     * Flush out index to index file from start.
     * */
    private void flushIndex()
    {
        // set index content offset, skip the index header.
        currentIndexOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
        // if root contains nodes, which means the tree is not empty,then write nodes.
        if (radix.getRoot().getSize() != 0) {
            writeRadix(radix.getRoot());
        }
    }
}
