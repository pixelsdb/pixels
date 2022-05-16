package io.pixelsdb.pixels.cache;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RadixToHashConverter {
    private final int HEADER_OFFSET = 8;
    private final MemoryMappedFile radixFile;
    private MemoryMappedFile hashFile = null;
    private final double loadFactor;
    private final int tableSize;
    // key-value pair: 12 + 12 = 24 bytes
    private final int kvSize = PixelsCacheKey.SIZE + PixelsCacheIdx.SIZE;
    private final byte[] kv = new byte[kvSize];
    private final ByteBuffer kvBuf = ByteBuffer.wrap(kv).order(ByteOrder.LITTLE_ENDIAN);
    private final ByteBuffer keyBuf = ByteBuffer.allocate(PixelsCacheKey.SIZE).order(ByteOrder.LITTLE_ENDIAN);




    static {
//        System.loadLibrary("radixToHash");
    }

    RadixToHashConverter(MemoryMappedFile radixFile, String hashFileName, double loadFactor) {
        this.radixFile = radixFile;
        this.loadFactor = loadFactor;
        this.tableSize = roundToTwoPower((int) (numKeys() / this.loadFactor)); // round to 2's power

        long mmapSize = (long) this.tableSize * kvSize + HEADER_OFFSET;
//        long mmapSize = 10240000000L;


        System.out.println("tableSize=" + tableSize + " loadFactor=" + loadFactor
                + " mmapSize=" + mmapSize + " kvSize=" + kvSize);
        try {
            this.hashFile = new MemoryMappedFile(hashFileName, mmapSize);
            this.hashFile.clear(); // set all memory to 0
            this.hashFile.setLong(0, tableSize);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public void close() {
        try {
            this.hashFile.unmap();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int roundToTwoPower(int v) {
        int ret = 1;
        while (ret < v) {
            ret *= 2;
        }
        return ret;
    }

    // return the number of keys in the radix tree
    private int numKeys() {
        return 512000; // TODO
    }

    private int hashcode(byte[] bytes) {
        int var1 = 1;

        for(int var3 = 0; var3 < bytes.length; ++var3) {
            var1 = 31 * var1 + bytes[var3];
        }

        return var1;
    }



    private void dfs(long nodeOffset, byte[] bigEndianKey, int byteMatched) {

        int currentNodeHeader = radixFile.getInt(nodeOffset);
        int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
        int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >>> 9;
        boolean isLeaf = ((currentNodeHeader >>> 31) & 1) > 0;
        byte[] nodeData = new byte[256 * 8 + 16];
        ByteBuffer childrenBuffer = ByteBuffer.wrap(nodeData).order(ByteOrder.LITTLE_ENDIAN);

        radixFile.getBytes(nodeOffset + 4, nodeData, 0, currentNodeChildrenNum * 8 + currentNodeEdgeSize);
        int edgeEndOffset = currentNodeChildrenNum * 8 + currentNodeEdgeSize;
        // add the edges
        for (int i = currentNodeChildrenNum * 8 + 1; i < edgeEndOffset; i++)
        {
            bigEndianKey[byteMatched++] = nodeData[i];
        }

        // reach a leaf
        if (byteMatched == PixelsCacheKey.SIZE) {
            assert isLeaf;

            // convert big endian to little endian
            // parse blockId, rgId, and columnId from keyBuf
            ByteBuffer bigKeyBuf = ByteBuffer.wrap(bigEndianKey).order(ByteOrder.BIG_ENDIAN);
            long blockId = bigKeyBuf.getLong();
            short rgId = bigKeyBuf.getShort();
            short columnId = bigKeyBuf.getShort();

            // construct a little endian key buf, this is good for C-compatibility
            keyBuf.position(0);
            keyBuf.putLong(blockId).putShort(rgId).putShort(columnId);

            int hash = hashcode(keyBuf.array()) & 0x7fffffff;
//            int bucket = hash % tableSize;
            int bucket = hash & (tableSize - 1); // initial bucket
            int offset = bucket * kvSize;
            hashFile.getBytes(offset + HEADER_OFFSET, kv, 0, this.kvSize);
            boolean valid = kvBuf.getLong(0) == 0 && kvBuf.getLong(8) == 0 && kvBuf.getLong(16) == 0; // all zero
            for(int i = 1; !valid; ++i) {
                bucket += i * i;
//                bucket = bucket % tableSize;
                bucket &= tableSize - 1;
                offset = bucket * kvSize;
                hashFile.getBytes(offset + HEADER_OFFSET, kv, 0, this.kvSize);
                valid = kvBuf.getLong(0) == 0 && kvBuf.getLong(8) == 0 && kvBuf.getLong(16) == 0;
            }
            // put the Cachekey
            kvBuf.position(0);
            kvBuf.putLong(blockId).putShort(rgId).putShort(columnId);

            // put the CacheIdx, aka value TODO: big endian or little endian?
            radixFile.getBytes(nodeOffset + 4 + (currentNodeChildrenNum * 8) + currentNodeEdgeSize,
                    kv, PixelsCacheKey.SIZE, PixelsCacheIdx.SIZE);

            // find a valid position, insert into hashFile
            hashFile.setBytes(offset + HEADER_OFFSET, kv);
            return;
        }

        // we reach a deadend, it is a bad/bug case
        if (nodeOffset == 0)
        {
            System.out.println("currentNodeOffset is NULL, but byteMatched=" + byteMatched);
            return;
        }

        // traverse the children
        childrenBuffer.position(0).limit(currentNodeChildrenNum * 8);
        for (int i = 0; i < currentNodeChildrenNum; i++)
        {
            // long is 8 byte, which is 64 bit
            long child = childrenBuffer.getLong();
            // first byte is matching byte
            byte leader = (byte) ((child >>> 56) & 0xFF);
            long matchingChildOffset = child & 0x00FFFFFFFFFFFFFFL;

            bigEndianKey[byteMatched++] = leader;
            dfs(matchingChildOffset, bigEndianKey, byteMatched);
            --byteMatched;
        }
    }

    public void rewriteRadixToHash() {
        // dfs traverse the radix tree
        long currentNodeOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
        byte[] bigEndianKey = new byte[PixelsCacheKey.SIZE];
        int byteMatched = 0;
        dfs(currentNodeOffset, bigEndianKey, byteMatched);
    }

    // rewrite radix index to an open addressing hash index
    public native void rewriteRadixToHash(long hashIndexFileAddr, long hashIndexFileSize);
}
