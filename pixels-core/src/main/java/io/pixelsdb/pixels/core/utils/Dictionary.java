/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.core.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

/**
 * @author hank
 * @date 8/13/22
 */
public class Dictionary
{
    private final int NUM_DICTIONARIES = 41;
    private final List<Map<KeyBuffer, Integer>> dictionaries;
    private int originalPosition = 0;

    public Dictionary(int initialCapacity)
    {
        int capacity = initialCapacity / NUM_DICTIONARIES;
        if (initialCapacity % NUM_DICTIONARIES > 0)
        {
            capacity++;
        }
        this.dictionaries = new ArrayList<>(NUM_DICTIONARIES);
        for (int i = 0; i < NUM_DICTIONARIES; ++i)
        {
            this.dictionaries.add(new HashMap<>(capacity));
        }
    }

    public int add(byte[] keyContent)
    {
        return add(keyContent, 0, keyContent.length);
    }

    public int add(byte[] keyContent, int offset, int length)
    {
        KeyBuffer keyBuffer = KeyBuffer.wrap(keyContent, offset, length);
        int dictId = keyBuffer.hashCode() % NUM_DICTIONARIES;
        Map<KeyBuffer, Integer> dict = this.dictionaries.get(dictId < 0 ? -dictId : dictId);
        Integer position = dict.get(keyBuffer);
        if (position != null)
        {
            return position;
        }
        dict.put(keyBuffer, this.originalPosition);
        return this.originalPosition++;
    }

    public int size()
    {
        return originalPosition;
    }

    public void clear()
    {
        for (Map<KeyBuffer, Integer> dict : this.dictionaries)
        {
            dict.clear();
        }
        this.dictionaries.clear();
    }

    public void visit(Visitor visitor) throws IOException
    {
        VisitorContextImpl visitorContext = new VisitorContextImpl();
        for (Map<KeyBuffer, Integer> dict : this.dictionaries)
        for (Map.Entry<KeyBuffer, Integer> entry : dict.entrySet())
        {
            KeyBuffer key = entry.getKey();
            visitorContext.set(key.keyContent, key.offset, key.length, entry.getValue());
            visitor.visit(visitorContext);
        }
    }

    private static class KeyBuffer implements Comparable<KeyBuffer>
    {
        private final byte[] keyContent;
        private final int offset;
        private final int length;

        public KeyBuffer(byte[] keyContent, int offset, int length)
        {
            this.keyContent = keyContent;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public int compareTo(KeyBuffer that)
        {
            int n = this.offset + (this.length < that.length ? this.length : that.length);
            int c;
            for (int i = this.offset, j = that.offset; i < n; ++i, ++j)
            {
                c = this.keyContent[i] - that.keyContent[j];
                if (c != 0)
                {
                    return c;
                }
            }
            return this.length - that.length;
        }

        @Override
        public boolean equals(Object o)
        {
            KeyBuffer that = (KeyBuffer) o;
            if (this.length != that.length)
            {
                return false;
            }
            int n = this.offset + this.length;
            for (int i = this.offset, j = that.offset; i < n; ++i, ++j)
            {
                if (this.keyContent[i] != that.keyContent[j])
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            int result = 31 + Integer.hashCode(this.length), n = this.offset + this.length;
            for (int i = this.offset; i < n; ++i)
                result = 31 * result + this.keyContent[i];
            return result;
        }

        public static KeyBuffer wrap(byte[] keyContent, int offset, int length)
        {
            return new KeyBuffer(keyContent, offset, length);
        }
    }

    /**
     * The information about each node.
     */
    public interface VisitorContext
    {
        /**
         * Get the position where the key was originally added.
         *
         * @return the number returned by add.
         */
        int getOriginalPosition();

        /**
         * Write the bytes for the string to the given output stream.
         *
         * @param out the stream to write to.
         * @throws IOException
         */
        void writeBytes(OutputStream out) throws IOException;

        /**
         * Get the number of bytes.
         *
         * @return the string's length in bytes
         */
        int getLength();
    }

    /**
     * The interface for visitors.
     */
    public interface Visitor
    {
        /**
         * Called once for each node of the tree in sort order.
         *
         * @param context the information about each node
         * @throws IOException
         */
        void visit(VisitorContext context) throws IOException;
    }

    public static class VisitorContextImpl implements VisitorContext
    {
        private byte[] key;
        private int offset;
        private int length;
        private int originalPosition;

        public int getOriginalPosition()
        {
            return originalPosition;
        }

        public void writeBytes(OutputStream out) throws IOException
        {
            out.write(key, offset, length);
        }

        public int getLength()
        {
            return length;
        }

        public void set(byte[] key, int offset, int length, int originalPosition)
        {
            this.key = key;
            this.offset = offset;
            this.length = length;
            this.originalPosition = originalPosition;
        }
    }
}
