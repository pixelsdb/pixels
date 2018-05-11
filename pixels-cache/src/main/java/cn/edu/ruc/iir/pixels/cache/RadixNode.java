package cn.edu.ruc.iir.pixels.cache;

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
    private int size = 0;                   // number of children
    private byte[] edge = null;
//    private RadixNode[] children = null;
    private Map<Byte, RadixNode> children;
    private PixelsCacheIdx value = null;
    public long offset;

    public RadixNode()
    {
//        this.children = new RadixNode[256];
        this.children = new HashMap<>();
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

//    public void addChild(RadixNode child, boolean overwrite)
//    {
//        byte firstByte = child.edge[0];
//        int index = firstByte + 128;
//        if (!overwrite && children[index] != null) {
//            return;
//        }
//        children[index] = child;
//        size++;
//    }

    public void addChild(RadixNode child, boolean overwrite)
    {
        byte firstByte = child.edge[0];
        if (!overwrite && children.containsKey(firstByte)) {
            return;
        }
        children.put(firstByte, child);
        size++;
    }

//    public void removeChild(RadixNode child)
//    {
//        byte firstByte = child.edge[0];
//        int index = firstByte + 128;
//        children[index] = null;
//        size--;
//    }

    public void removeChild(RadixNode child)
    {
        byte firstByte = child.edge[0];
        children.put(firstByte, null);
        size--;
    }

//    public RadixNode getChild(byte firstByte)
//    {
//        int index = firstByte + 128;
//        return children[index];
//    }

    public RadixNode getChild(byte firstByte)
    {
        return children.get(firstByte);
    }

//    public void setChildren(RadixNode[] children, int size)
//    {
//        this.children = children;
//        this.size = size;
//    }

    public void setChildren(Map<Byte, RadixNode> children, int size)
    {
        this.children = children;
        this.size = size;
    }

//    public RadixNode[] getChildren()
//    {
//        return children;
//    }

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

    /**
     * Content length in bytes
     * */
    public int getLengthInBytes()
    {
        int len = 2 + edge.length;
        len += 8 * children.size();
        if (isKey) {
            len += PixelsCacheIdx.SIZE;
        }
        return len;
    }
}
