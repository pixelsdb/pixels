/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core.encoding;

import io.pixelsdb.pixels.core.utils.DynamicByteArray;
import io.pixelsdb.pixels.core.utils.DynamicIntArray;
import io.pixelsdb.pixels.core.utils.EncodingUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;

/**
 * This id derived from the StringRedBlockTree implementation in Apache Orc.
 * A red-black tree that stores strings. The strings are stored as UTF-8 bytes
 * and an offset for each entry.
 *
 * @author guodong
 * @author hank
 */
public class StringRedBlackTree extends RedBlackTree implements Dictionary
{
    private final DynamicByteArray byteArray = new DynamicByteArray();
    private final DynamicIntArray keyOffsets;
    private byte[] keyBytes;
    private int keyLength;

    public StringRedBlackTree(int initialCapacity)
    {
        super(initialCapacity);
        keyOffsets = new DynamicIntArray(initialCapacity);
    }

    @Override
    public int add(String value)
    {
        try
        {
            ByteBuffer bb = EncodingUtils.encodeString(value, true);
            keyBytes = bb.array();
            keyLength = bb.limit();
            if (add())
            {
                keyOffsets.add(byteArray.add(keyBytes, 0, keyLength));
            }
            return lastAdd;
        }
        catch (CharacterCodingException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public int add(byte[] keyContent)
    {
        return add(keyContent, 0, keyContent.length);
    }

    @Override
    public int add(byte[] value, int offset, int length)
    {
        setCapacity(length, false);
        System.arraycopy(value, offset, keyBytes, 0, length);
        this.keyLength = length;
        if (add())
        {
            keyOffsets.add(byteArray.add(keyBytes, 0, keyLength));
        }
        return lastAdd;
    }

    private void setCapacity(int len, boolean keepData)
    {
        if (keyBytes == null || keyBytes.length < len)
        {
            if (keyBytes != null && keepData)
            {
                keyBytes = Arrays.copyOf(keyBytes, Math.max(len, keyLength << 1));
            }
            else
            {
                keyBytes = new byte[len];
            }
        }
    }

    // compare newKey with position specified key
    @Override
    protected int compareValue(int position)
    {
        int start = keyOffsets.get(position);
        int end;
        if (position + 1 == keyOffsets.size())
        {
            end = byteArray.size();
        }
        else
        {
            end = keyOffsets.get(position + 1);
        }
        return byteArray.compare(keyBytes, 0, keyLength,
                start, end - start);
    }

    /**
     * Visit the items (keys) recursively in a deep-first manner.
     * @param node the root key to start visiting
     * @param visitor the visitor
     * @param context the context instance that is reused by the visitor
     * @throws IOException
     */
    private void recurse(int node, Visitor visitor, VisitorContextImpl context) throws IOException
    {
        if (node != NULL)
        {
            recurse(getLeft(node), visitor, context);
            context.setKeyPosition(node);
            visitor.visit(context);
            recurse(getRight(node), visitor, context);
        }
    }

    /**
     * Visit all the nodes in the tree in sorted order.
     * @param visitor the action to be applied to each node
     * @throws IOException
     */
    @Override
    public void visit(Visitor visitor)
            throws IOException
    {
        /*
         * Issue #498:
         * Previously, we visit the keys in the rea-black tree in a deep-first manner like this:
         * recurse(root, visitor, new VisitorContextImpl());
         *
         * This is not necessary anymore, as we plan to remove the orders array from dictionary encoding
         * and store the dictionary keys in the order they were inserted.
         */
        int size = this.keyOffsets.size();
        VisitorContextImpl visitorContext = new VisitorContextImpl();
        for (int position = 0; position < size; ++position)
        {
            visitorContext.setKeyPosition(position);
            visitor.visit(visitorContext);
        }
    }

    /**
     * Reset the dictionary to empty.
     */
    @Override
    public void clear()
    {
        super.clear();
        byteArray.clear();
        keyOffsets.clear();
    }

    private class VisitorContextImpl implements VisitorContext
    {
        private int keyPosition;
        private int start;
        private int end;

        public int getKeyPosition()
        {
            return keyPosition;
        }

        @Override
        public void writeBytes(OutputStream out)
                throws IOException
        {
            byteArray.write(out, start, end - start);
        }

        @Override
        public int getLength()
        {
            return end - start;
        }

        void setKeyPosition(int keyPosition)
        {
            this.keyPosition = keyPosition;
            start = keyOffsets.get(keyPosition);
            if (keyPosition + 1 == keyOffsets.size())
            {
                end = byteArray.size();
            }
            else
            {
                end = keyOffsets.get(keyPosition + 1);
            }
        }
    }
}
