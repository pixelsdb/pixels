package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.cache.mq.MappedBusReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * pixels cache manager.
 * This is a scheduled thread.
 *
 * @author guodong
 */
public class PixelsCacheManager
        extends Thread
{
    private final static short READABLE = 0;
    private final static short WRITE = 1;
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final MappedBusReader mqReader;
    private final List<ColumnletId> columnletIds;
    private final PixelsRadix radix;
    private long currentIndexOffset = 0L;

    public PixelsCacheManager(MemoryMappedFile cacheFile,
                              MemoryMappedFile indexFile,
                              MappedBusReader mqReader,
                              PixelsRadix radix)
    {
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.mqReader = mqReader;
        this.columnletIds = new LinkedList<>();
        this.radix = radix;
    }

    @Override
    public void run()
    {
        // set rwFlag as write
        indexFile.putShortVolatile(0, (short) 1);
        // wait until readerCount is 0
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 3000) {
            if (indexFile.getShortVolatile(2) == 0) {
                break;
            }
        }
        indexFile.putShortVolatile(2, (short) 0);
        // collect cache missing messages from mq, and sort cache missings by their missing counts
        try
        {
            mqReader.open();
            while (mqReader.next()) {
                ColumnletId columnletId = new ColumnletId();
                mqReader.readMessage(columnletId);
                int index = columnletIds.indexOf(columnletId);
                if (index >= 0) {
                    ColumnletId target = columnletIds.get(index);
                    target.missingCount++;
                }
                else {
                    columnletIds.add(columnletId);
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        // read all columnlets in cache, and sort by their access counts
        traverseRadix(columnletIds);
        columnletIds.sort(Comparator.comparingInt(o -> o.missingCount));
        // decide which columnlets to evict and which ones to insert
        evict(columnletIds);
        // get offsets of all remaining cached columnlets, sort by their offsets
        List<ColumnletId> remainingCaches = new LinkedList<>();
        for (ColumnletId columnletId : columnletIds) {
            if (columnletId.cached) {
                remainingCaches.add(columnletId);
            }
        }
        remainingCaches.sort(Comparator.comparingLong(o -> o.cacheOffset));
        // write all remaining cached columnlets by order, reset their counts
        compact(remainingCaches);
        // flush index
        flushIndex();
        // set rwFlag as readable
        indexFile.putShortVolatile(0, READABLE);
        // todo read missing columnlets and append them into cache file, and change radix accordingly.

        // set rwFlag as write
        indexFile.putShortVolatile(0, WRITE);
        // wait until readerCount is 0
        start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 3000) {
            if (indexFile.getShortVolatile(2) == 0) {
                break;
            }
        }
        indexFile.putShortVolatile(2, (short) 0);
        // flush index
        flushIndex();
        // increase version
        indexFile.getAndAddLong(4, 1);
        // set raFlag as readable
        indexFile.putShortVolatile(0, READABLE);
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
