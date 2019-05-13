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
    private Map<Byte, RadixNode> children;
    private PixelsCacheIdx value = null;
    public long offset = 0L;

    public RadixNode()
    {
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

    public void addChild(RadixNode child, boolean overwrite)
    {
        byte firstByte = child.edge[0];
        if (!overwrite && children.containsKey(firstByte))
        {
            return;
        }
        children.put(firstByte, child);
        size++;
    }

    public void removeChild(RadixNode child)
    {
        byte firstByte = child.edge[0];
        children.put(firstByte, null);
        size--;
    }

    public RadixNode getChild(byte firstByte)
    {
        return children.get(firstByte);
    }

    public void setChildren(Map<Byte, RadixNode> children, int size)
    {
        this.children = children;
        this.size = size;
    }

    public Map<Byte, RadixNode> getChildren()
    {
        return children;
    }

    public void setValue(PixelsCacheIdx value)
    {
        this.value = value;
        if (value == null)
        {
            this.isKey = false;
        }
        else
        {
            this.isKey = true;
        }
    }

    public PixelsCacheIdx getValue()
    {
        return value;
    }

    /**
     * Content length in bytes
     */
    public int getLengthInBytes()
    {
        int len = 4 + edge.length;  // header
        len += 1 * children.size(); // leaders
        len += 8 * children.size(); // offsets
        // value
        if (isKey)
        {
            len += PixelsCacheIdx.SIZE;
        }
        return len;
    }
}
