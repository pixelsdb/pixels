package io.pixelsdb.pixels.cache;

import java.nio.ByteBuffer;

public class CacheIndexSerializer {

    private final MemoryMappedFile indexFile;

    static {
        System.loadLibrary("cache");
    }

    CacheIndexSerializer(MemoryMappedFile indexFile) {
        this.indexFile = indexFile;
    }

    // traverse the index
    public native void traverse();

    public void _traverse() {
        System.out.println("normal -----------------------");
        byte[] nodeData = new byte[256 * 8 + 16];
        ByteBuffer childrenBuffer = ByteBuffer.wrap(nodeData);


        long currentNodeOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
        int currentNodeHeader = indexFile.getInt(currentNodeOffset);
        int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
        int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
        System.out.println("currentNodeHeader=" + String.format("0x%08X", currentNodeHeader));
        System.out.println("currentNodeChildrenNum: " + currentNodeChildrenNum + " currentNodeEdgeSize: " + currentNodeEdgeSize);
        if (currentNodeChildrenNum == 0 && currentNodeEdgeSize == 0)
        {
            return;
        }
        indexFile.getBytes(currentNodeOffset + 4, nodeData, 0, currentNodeChildrenNum * 8);

        long matchingChildOffset = 0L;
        childrenBuffer.position(0);
        childrenBuffer.limit(currentNodeChildrenNum * 8);
        // linearly scan all the children
        for (int i = 0; i < currentNodeChildrenNum; i++)
        {
            // long is 8 byte, which is 64 bit
            long child = childrenBuffer.getLong();
            // first byte is matching byte
            byte leader = (byte) ((child >>> 56) & 0xFF);
            matchingChildOffset = child & 0x00FFFFFFFFFFFFFFL;
            System.out.println("child=" + String.format("0x%08X", child) + " leader="+leader + " childOffset="+matchingChildOffset);
        }

    }
}
