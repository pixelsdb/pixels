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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hank
 * @date 8/13/22
 */
public class HashTableDictionary implements Dictionary
{
    private final int NUM_DICTIONARIES = 41;
    private final List<Map<KeyBuffer, Integer>> dictionaries;
    private int originalPosition = 0;

    public HashTableDictionary(int initialCapacity)
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

    @Override
    public int add(String key)
    {
        try
        {
            ByteBuffer buffer = EncodingUtils.encodeString(key, true);
            return add(buffer.array(), 0, buffer.limit());
        } catch (CharacterCodingException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public int add(byte[] key)
    {
        return add(key, 0, key.length);
    }

    @Override
    public int add(byte[] key, int offset, int length)
    {
        KeyBuffer keyBuffer = KeyBuffer.wrap(key, offset, length);
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

    @Override
    public int size()
    {
        return originalPosition;
    }

    @Override
    public void clear()
    {
        for (Map<KeyBuffer, Integer> dict : this.dictionaries)
        {
            dict.clear();
        }
        this.dictionaries.clear();
    }

    @Override
    public void visit(Dictionary.Visitor visitor) throws IOException
    {
        VisitorContextImpl visitorContext = new VisitorContextImpl();
        for (Map<KeyBuffer, Integer> dict : this.dictionaries)
        for (Map.Entry<KeyBuffer, Integer> entry : dict.entrySet())
        {
            KeyBuffer key = entry.getKey();
            visitorContext.setKey(key.bytes, key.offset, key.length, entry.getValue());
            visitor.visit(visitorContext);
        }
    }

    private static class KeyBuffer implements Comparable<KeyBuffer>
    {
        private final byte[] bytes;
        private final int offset;
        private final int length;

        public KeyBuffer(byte[] bytes, int offset, int length)
        {
            this.bytes = bytes;
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
                c = this.bytes[i] - that.bytes[j];
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
                if (this.bytes[i] != that.bytes[j])
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
                result = 31 * result + this.bytes[i];
            return result;
        }

        public static KeyBuffer wrap(byte[] keyContent, int offset, int length)
        {
            return new KeyBuffer(keyContent, offset, length);
        }
    }

    private class VisitorContextImpl implements Dictionary.VisitorContext
    {
        private byte[] key;
        private int offset;
        private int length;
        private int keyPosition;

        @Override
        public int getKeyPosition()
        {
            return keyPosition;
        }

        @Override
        public void writeBytes(OutputStream out) throws IOException
        {
            out.write(key, offset, length);
        }

        @Override
        public int getLength()
        {
            return length;
        }

        public void setKey(byte[] key, int offset, int length, int keyPosition)
        {
            this.key = key;
            this.offset = offset;
            this.length = length;
            this.keyPosition = keyPosition;
        }
    }
}
