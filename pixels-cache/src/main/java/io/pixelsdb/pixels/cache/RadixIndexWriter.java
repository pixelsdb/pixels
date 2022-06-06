package io.pixelsdb.pixels.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;

// serialize to a memory mapped file
public class RadixIndexWriter implements CacheIndexWriter {
    private final static Logger logger = LogManager.getLogger(RadixIndexWriter.class);

    private final PixelsRadix radix;
    private final MemoryMappedFile out;
    private long currentIndexOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
    private long allocatedIndexOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;

    private final ByteBuffer nodeBuffer = ByteBuffer.allocate(8 * 256);
    private final ByteBuffer cacheIdxBuffer = ByteBuffer.allocate(PixelsCacheIdx.SIZE);



    RadixIndexWriter(MemoryMappedFile out) {
        this.radix = new PixelsRadix();
        this.out = out;
    }

    /**
     * Write radix tree node.
     */
    private boolean writeRadix(RadixNode node)
    {
        if (!flushNode(node)) return false;
        boolean ret = true;
        for (RadixNode n : node.getChildren().values())
        {
            if (!writeRadix(n)) {
                ret = false;
                break;
            }
        }
        return ret;
    }

    /**
     * Flush node content to the index file based on {@code currentIndexOffset}.
     * Header(4 bytes) + [Child(8 bytes)]{n} + edge(variable size) + value(optional).
     * Header: isKey(1 bit) + edgeSize(22 bits) + childrenSize(9 bits)
     * Child: leader(1 byte) + child_offset(7 bytes)
     */
    private boolean flushNode(RadixNode node)
    {
        nodeBuffer.clear();
        if (currentIndexOffset >= out.getSize())
        {
            logger.debug("Offset exceeds index size. Break. Current size: " + currentIndexOffset);
            return false;
        }
        if (node.offset == 0)
        {
            node.offset = currentIndexOffset;
        }
        else
        {
            currentIndexOffset = node.offset;
        }
        allocatedIndexOffset += node.getLengthInBytes();
        int header = 0;
        int edgeSize = node.getEdge().length;
        header = header | (edgeSize << 9);
        int isKeyMask = 1 << 31;
        if (node.isKey())
        {
            header = header | isKeyMask;
        }
        header = header | node.getChildren().size();
        out.setInt(currentIndexOffset, header);  // header
        currentIndexOffset += 4;
        for (Byte key : node.getChildren().keySet())
        {   // children
            RadixNode n = node.getChild(key);
            int len = n.getLengthInBytes();
            n.offset = allocatedIndexOffset;
            allocatedIndexOffset += len;
            long childId = 0L;
            childId = childId | ((long) key << 56);  // leader
            childId = childId | n.offset;  // offset
            nodeBuffer.putLong(childId);
//            indexFile.putLong(currentIndexOffset, childId);
//            currentIndexOffset += 8;
        }
        byte[] nodeBytes = new byte[node.getChildren().size() * 8];
        nodeBuffer.flip();
        nodeBuffer.get(nodeBytes);
        out.setBytes(currentIndexOffset, nodeBytes); // children
        currentIndexOffset += nodeBytes.length;
        out.setBytes(currentIndexOffset, node.getEdge()); // edge
        currentIndexOffset += node.getEdge().length;
        if (node.isKey())
        {  // value
            node.getValue().getBytes(cacheIdxBuffer);
            out.setBytes(currentIndexOffset, cacheIdxBuffer.array());
            currentIndexOffset += 12;
        }
        return true;
    }

    @Override
    public void put(PixelsCacheKey cacheKey, PixelsCacheIdx cacheIdx) {
        this.radix.put(cacheKey, cacheIdx);
    }

    @Override
    public void clear() {
        this.radix.removeAll();
    }

    @Override
    public long flush() {
        // if root contains nodes, which means the tree is not empty,then write nodes.
        if (radix.getRoot().getSize() != 0)
        {
            if (writeRadix(radix.getRoot())) return currentIndexOffset;
            else return -1;
        }
        return currentIndexOffset;

    }
}
