package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
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

    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final FileSystem fs;
    private final PixelsRadix radix;
    private final EtcdUtil etcdUtil;
    private long currentIndexOffset;

    private PixelsCacheWriter(MemoryMappedFile cacheFile,
                             MemoryMappedFile indexFile,
                             FileSystem fs,
                             PixelsRadix radix,
                             EtcdUtil etcdUtil)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.fs = fs;
        this.radix = radix;
        this.etcdUtil = etcdUtil;
    }

    public static class Builder
    {
        private String builderCacheLocation = "";
        private long builderCacheSize;
        private String builderIndexLocation = "";
        private long builderIndexSize;
        private FileSystem builderFS;
        private boolean builderOverwrite = true;

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

        public PixelsCacheWriter build()
                throws Exception
        {
            MemoryMappedFile cacheFile = new MemoryMappedFile(builderCacheLocation, builderCacheSize);
            MemoryMappedFile indexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);
            PixelsRadix radix;
            // check if cache(index) file exists.
            //   if overwrite is true, then create a new cache file and an index file.
            //   else, create the radix tree from the existing cache(index) file.
            if (builderOverwrite) {
                radix = new PixelsRadix();
            }
            else if (PixelsCacheUtil.checkMagic(indexFile) && PixelsCacheUtil.checkMagic(cacheFile)){
                radix = PixelsCacheUtil.getRadix(indexFile);
            }
            else {
                radix = new PixelsRadix();
            }
            // todo check null of all parameters
            EtcdUtil etcdUtil = EtcdUtil.Instance();

            return new PixelsCacheWriter(cacheFile, indexFile, builderFS, radix, etcdUtil);
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

    public void updateAll(int version, Layout layout)
    {
        try {
            // get the caching file list
            String fileStr = etcdUtil.getKeyValue("location_" + version + "_node_id").getValue().toStringUtf8();
            String[] files = fileStr.split(";"); // todo split is inefficient
            internalUpdate(version, layout, files);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void internalUpdate(int version, Layout layout, String[] files)
            throws IOException
    {
        // get the new caching layout
        String compactStr = layout.getCompact();
        Compact compact = (Compact) JSON.parse(compactStr);
        int cacheBorder = compact.getCacheBorder();
        List<String> cacheColumnletOrders = compact.getColumnletOrder().subList(0, cacheBorder);
        // set rwFlag as write
        PixelsCacheUtil.setIndexRW(indexFile, (short) 1);
        // wait until readerCount is 0
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 3000) {
            if (PixelsCacheUtil.getIndexReaderCount(indexFile) == 0) {
                break;
            }
        }
        PixelsCacheUtil.setIndexReaderCount(indexFile, (short) 0);
        // update cache content
        radix.removeAll();
        long cacheOffset = 0L;
        for (String file : files)
        {
            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(fs, new Path(file));
            int[] physicalLens = new int[cacheColumnletOrders.size()];
            long[] physicalOffsets = new long[cacheColumnletOrders.size()];
            // update radix
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
                radix.put(new PixelsCacheKey(file, rowGroupId, columnId),
                          new PixelsCacheIdx(cacheOffset, physicalLens[i]));
                cacheOffset += physicalLens[i];
            }
            // update cache content
            cacheOffset = 0L;
            for (int i = 0; i < cacheColumnletOrders.size(); i++)
            {
                byte[] columnlet = pixelsPhysicalReader.read(physicalOffsets[i], physicalLens[i]);
                cacheFile.putBytes(cacheOffset, columnlet);
                cacheOffset += physicalLens[i];
            }
        }
        // update cache version
        PixelsCacheUtil.setIndexVersion(indexFile, version);
        // flush index
        flushIndex();
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
        flushNode(node);
        for (RadixNode n : node.getChildren().values()) {
            writeRadix(n);
        }
    }

    /**
     * Flush node content to the index file based on {@code currentIndexOffset}.
     * Header(2 bytes) + [Child(1 byte)]{n} + edge(variable size) + value(optional).
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
     * Flush out index to index file from start.
     * */
    private void flushIndex()
    {
        currentIndexOffset = 0;
        if (radix.getRoot().getSize() != 0) {
            writeRadix(radix.getRoot());
        }
    }
}
