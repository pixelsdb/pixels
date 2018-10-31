package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.cache.mq.MappedBusReader;
import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheWriter
{
    private final static short READABLE = 0;
    private final static short WRITE = 1;

    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final MappedBusReader mqReader;
    private final List<ColumnletId> columnletIds;
    private final FileSystem fs;
    private final PixelsRadix radix;
    private final String schema;
    private final String table;
    private final MetadataService metadataService;
    private final EtcdUtil etcdUtil;
    private long currentIndexOffset;

    public PixelsCacheWriter(MemoryMappedFile cacheFile,
                             MemoryMappedFile indexFile,
                             MappedBusReader mqReader,
                             FileSystem fs,
                             PixelsRadix radix,
                             String schema,
                             String table,
                             String metaHost,
                             int metaPort)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.mqReader = mqReader;
        this.columnletIds = new LinkedList<>();
        this.fs = fs;
        this.radix = radix;
        this.schema = schema;
        this.table = table;
        this.metadataService = new MetadataService(metaHost, metaPort);
        this.etcdUtil = EtcdUtil.Instance();
    }

    public void updateAll(int version)
    {
        try {
            // get the matched layout
            List<Layout> layouts = metadataService.getLayouts(schema, table);
            Layout chosenLayout = null;
            for (Layout layout : layouts)
            {
                if (layout.getVersion() == version) {
                    chosenLayout = layout;
                    break;
                }
            }
            if (chosenLayout == null) {
                // no matching layout
                return;
            }
            // get the caching file list
            String fileStr = etcdUtil.getKeyValue("location_" + version + "_node_id").getValue().toStringUtf8();
            String[] files = fileStr.split(";"); // todo split is inefficient
            internalUpdate(version, chosenLayout, files);
        }
        catch (MetadataException | IOException e) {
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

    private void run()
    {
        // collect cache missing messages from mq, and sort caches by their missing counts
//        try {
//            mqReader.open();
//            while (mqReader.next()) {
//                ColumnletId columnletId = new ColumnletId();
//                mqReader.readMessage(columnletId);
//                int index = columnletIds.indexOf(columnletId);
//                if (index >= 0) {
//                    ColumnletId target = columnletIds.get(index);
//                    target.missingCount++;
//                }
//                else {
//                    columnletIds.add(columnletId);
//                }
//            }
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
        // read all columnlets in cache, and sort by their access counts
//        traverseRadix(columnletIds);
//        columnletIds.sort(Comparator.comparingInt(o -> o.missingCount));
        // decide which columnlets to evict and which ones to insert
//        evict(columnletIds);
        // get offsets of all remaining cached columnlets, sort by their offsets
//        List<ColumnletId> remainingCaches = new LinkedList<>();
//        for (ColumnletId columnletId : columnletIds) {
//            if (columnletId.cached) {
//                remainingCaches.add(columnletId);
//            }
//        }
//        remainingCaches.sort(Comparator.comparingLong(o -> o.cacheOffset));
        // write all remaining cached columnlets by order, reset their counts
//        compact(remainingCaches);
        // flush index
//        flushIndex();

        // todo read missing columnlets and append them into cache file, and change radix accordingly.

        // set rwFlag as write
//        indexFile.putShortVolatile(0, WRITE);
        // wait until readerCount is 0
//        start = System.currentTimeMillis();
//        while (System.currentTimeMillis() - start < 3000) {
//            if (indexFile.getShortVolatile(2) == 0) {
//                break;
//            }
//        }
//        indexFile.putShortVolatile(2, (short) 0);
        // flush index
//        flushIndex();
        // increase version
//        indexFile.getAndAddLong(4, 1);
        // set raFlag as readable
//        indexFile.putShortVolatile(0, READABLE);
    }

    /**
     * Evict caches from radix tree.
     * Remove cache keys evicted or not to be inserted from cacheKeys list.
     * */
    private void evict(List<ColumnletId> cachedColumnlets)
    {}

    /**
     * Compact remaining caches in the cache file.
     * */
    private void compact(List<ColumnletId> remainingColumnlets)
    {
        long currentOffset = 0;
        for (ColumnletId columnletId : remainingColumnlets) {
            long columnletOffset = columnletId.cacheOffset;
            int columnletLength = columnletId.cacheLength;
            if (columnletOffset != currentOffset) {
                byte[] columnlet = new byte[columnletLength];
                cacheFile.getBytes(columnletOffset, columnlet, 0, columnletLength);
                cacheFile.putBytes(currentOffset, columnlet);
            }
            currentOffset += columnletLength;
        }
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
