package cn.edu.ruc.iir.pixels.cache;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsRadix
{
    private final DynamicArray<RadixNode> nodes;    // tree nodes.

    public PixelsRadix()
    {
        this.nodes = new DynamicArray<>();
        nodes.add(new RadixNode());                 // add root node
    }

    public boolean remove(PixelsCacheKey cacheKey)
    {
        return false;
    }

    public void put(PixelsCacheKey cacheKey, PixelsCacheIdx cacheItem)
    {
        RadixNode root = nodes.get(0);
        // if tree is empty, put it as compressed root
        if (root.getSize() == 0) {
            RadixNode node = new RadixNode();
            node.setKey(true);
            node.setNull(false);
            node.setCompressed(true);
            byte[] key = cacheKey.getBytes();
            byte[] value = cacheItem.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(key.length + Integer.BYTES + value.length);
            buffer.put(key);
            buffer.putInt(-1);
            buffer.put(value);
            node.setData(buffer.array());
            node.setSize(key.length);
            nodes.set(0, node);
        }
    }

    public void putIfAbsent(PixelsCacheKey cacheKey, PixelsCacheIdx cacheItem)
    {

    }

    public PixelsCacheIdx get(PixelsCacheKey cacheKey)
    {
        RadixNode root = nodes.get(0);
        // if tree is empty, return null
        if (root.getSize() == 0) {
            return null;
        }
        // if tree is not empty, continue search
        int keyLen = PixelsCacheKey.SIZE;
        int index = 0;
        while (index < keyLen) {
            // if matching, continue, and increment index
            index++;
            // if not matching, break
        }

        return null;
    }
}
