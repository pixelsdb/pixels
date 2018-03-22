package cn.edu.ruc.iir.pixels.cache;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * pixels
 *
 * @author guodong
 */
public class RadixNode
{
    private boolean isKey = false;          // does this node contains a key? [1 bit]
    private boolean isNull = true;          // associated value is NULL [1 bit]
    private boolean isCompressed = false;   // node is compressed [1 bit]
    private int size = 0;                   // number of children, or length of compressed bytes [29 bit]
    private byte[] edge = null;
//    private RadixNode[] children = null;
    private Map<Byte, RadixNode> children = new HashMap<>();
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
    }

    public boolean isKey()
    {
        return isKey;
    }

    public void setKey(boolean key)
    {
        isKey = key;
    }

    public boolean isNull()
    {
        return isNull;
    }

    public void setNull(boolean aNull)
    {
        isNull = aNull;
    }

    public boolean isCompressed()
    {
        return isCompressed;
    }

    public void setCompressed(boolean compressed)
    {
        isCompressed = compressed;
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
        if (!overwrite && children.containsKey(firstByte)) {
            return;
        }
        children.put(firstByte, child);
    }

    public void removeChild(RadixNode child)
    {
        byte firstByte = child.edge[0];
        children.remove(firstByte);
    }

    public RadixNode getChild(byte firstByte)
    {
        return children.get(firstByte);
    }

    public void setChildren(Map<Byte, RadixNode> children)
    {
        this.children = children;
    }

    public Map<Byte, RadixNode> getChildren()
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
        if (isNull) {
            header = header | (1 << 30);
        }
        if (isCompressed) {
            header = header | ( 1 <<29);
        }
        header = header | size;

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
