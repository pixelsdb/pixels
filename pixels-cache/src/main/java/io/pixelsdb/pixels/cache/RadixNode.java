/*
 * Copyright 2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * @author guodong, hank
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
        if (children.put(firstByte, child) == null)
        {
            /**
             * Issue #72:
             * size was increased no matter firstByte exists or not,
             * so that size could become larger than exact children number.
             * Fix it in this issue.
             */
            size++;
        }
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
        /**
         * Issue #72:
         * leader is already contained in the highest 8 bits in each children (id),
         * no needed to allocate memory space for leaders.
         */
        // len += 1 * children.size(); // leaders
        len += 8 * children.size(); // offsets
        // value
        if (isKey)
        {
            len += PixelsCacheIdx.SIZE;
        }
        return len;
    }
}
