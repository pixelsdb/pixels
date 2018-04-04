package cn.edu.ruc.iir.pixels.cache;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class RadixNode
{
    private boolean isKey = false;          // does this node contains a key? [1 bit]
    private int size = 0;                   // number of children
    private byte[] edge = null;
    private RadixNode[] children = null;
//    private Map<Byte, RadixNode> children = new HashMap<>();
    private PixelsCacheIdx value = null;

    // if node is not compressed, size is the num of children,
    // data has `size` bytes of content, one for each child byte, and `size` pointers, point to each child node.
    // [abc][a-ptr][b-ptr][c-ptr][value-ptr?]

    // if node is compressed, size is the length of compressed bytes,
    // data has `size` bytes of content stored represent a sequence of successive nodes,
    // [xyz][z-ptr][value-ptr?]

    // if the node has an associated key (isKey=1) and is not null (isNull=0),
    // then after the node pointers, an additional value pointer is present.
//    private byte[] data = null;             // node data

    public RadixNode()
    {
        this.children = new RadixNode[256];
    }

    public boolean isKey()
    {
        return isKey;
    }

    public void setKey(boolean key)
    {
        isKey = key;
    }

    public int getSize()
    {
        return size;
    }

    public void setSize(int size)
    {
        this.size = size;
    }

    public void setEdge(byte[] edge)
    {
        this.edge = edge;
    }

    public byte[] getEdge()
    {
        return edge;
    }

    public void addChild(RadixNode child, boolean overwrite)
    {
        byte firstByte = child.edge[0];
        int index = firstByte + 128;
        if (!overwrite && children[index] != null) {
            return;
        }
        children[index] = child;
        size++;
    }

    public void removeChild(RadixNode child)
    {
        byte firstByte = child.edge[0];
        int index = firstByte + 128;
        children[index] = null;
        size--;
    }

    public RadixNode getChild(byte firstByte)
    {
        int index = firstByte + 128;
        return children[index];
    }

    public void setChildren(RadixNode[] children, int size)
    {
        this.children = children;
        this.size = size;
    }

    public RadixNode[] getChildren()
    {
        return children;
    }

    public void setValue(PixelsCacheIdx value)
    {
        this.value = value;
    }

    public PixelsCacheIdx getValue()
    {
        return value;
    }

    public byte[] getBytes()
    {
        int header = 0;
        if (isKey) {
            header = header | (1 << 31);
        }

        int bytesSize = Integer.BYTES;
//        if (data != null) {
//            bytesSize += data.length;
//        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytesSize);
        byteBuffer.putInt(header);
//        if (data != null) {
//            byteBuffer.put(data);
//        }

        return byteBuffer.array();
    }
}
