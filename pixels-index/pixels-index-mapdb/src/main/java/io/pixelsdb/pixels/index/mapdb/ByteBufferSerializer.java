/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.index.mapdb;

import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import net.jpountz.xxhash.XXHash32;
import org.mapdb.CC;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

/**
 * This serializer is only used to serialize and deserialize the byte buffer backed by a byte array on heap.
 * We assume the byte buffer starts from the first byte of the backing array.
 * @author hank
 * @create 2025-11-25
 */
public class ByteBufferSerializer implements GroupSerializer<ByteBuffer>
{

    private static final XXHash32 HASHER = CC.HASH_FACTORY.hash32();

    @Override
    public void serialize(DataOutput2 out, ByteBuffer value) throws IOException
    {
        checkValidBuffer(value);
        out.packInt(value.limit());
        out.write(value.array(), 0, value.limit());
    }

    @Override
    public ByteBuffer deserialize(DataInput2 in, int available) throws IOException
    {
        int size = in.unpackInt();
        byte[] ret = new byte[size];
        in.readFully(ret);
        return ByteBuffer.wrap(ret);
    }

    @Override
    public boolean isTrusted()
    {
        return true;
    }

    @Override
    public boolean equals(ByteBuffer a1, ByteBuffer a2)
    {
        checkValidBuffer(a1);
        checkValidBuffer(a2);
        a1.position(0);
        a2.position(0);
        return a1.equals(a2);
    }

    public int hashCode(ByteBuffer value, int seed)
    {
        return HASHER.hash(value, 0, value.limit(), seed);
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        checkValidBuffer(o1);
        checkValidBuffer(o2);
        if (o1 == o2)
        {
            return 0;
        }
        o1.position(0);
        o2.position(0);
        // Issue #1214: should not use o1.compareTo(o2), as ByteBuffer.compareTo compares signed bytes.
        final int len = Math.min(o1.limit(), o2.limit());
        for (int i = 0; i < len; i++)
        {
            int b1 = o1.get(i) & 0xFF;
            int b2 = o2.get(i) & 0xFF;
            if (b1 != b2)
            {
                return b1 - b2;
            }
        }
        return o1.limit() - o2.limit();
    }

    @Override
    public int valueArraySearch(Object keys, ByteBuffer key)
    {
        checkValidBuffer(key);
        key.position(0);
        return Arrays.binarySearch((ByteBuffer[])keys, key);
    }

    @Override
    public int valueArraySearch(Object keys, ByteBuffer key, Comparator comparator)
    {
        checkValidBuffer(key);
        key.position(0);
        return Arrays.binarySearch((ByteBuffer[])keys, key, comparator);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException
    {
        ByteBuffer[] vals2 = (ByteBuffer[]) vals;
        out.packInt(vals2.length);
        for(ByteBuffer b : vals2)
        {
            serialize(out, b);
        }
    }

    @Override
    public ByteBuffer[] valueArrayDeserialize(DataInput2 in, int size) throws IOException
    {
        int s = in.unpackInt();
        ByteBuffer[] ret = new ByteBuffer[s];
        for(int i = 0; i < s; ++i)
        {
            ret[i] = deserialize(in, -1);
        }
        return ret;
    }

    @Override
    public ByteBuffer valueArrayGet(Object vals, int pos)
    {
        return ((ByteBuffer[]) vals)[pos];
    }

    @Override
    public int valueArraySize(Object vals)
    {
        return ((ByteBuffer[]) vals).length;
    }

    @Override
    public ByteBuffer[] valueArrayEmpty()
    {
        return new ByteBuffer[0];
    }

    @Override
    public ByteBuffer[] valueArrayPut(Object vals, int pos, ByteBuffer newValue)
    {
        checkValidBuffer(newValue);
        ByteBuffer[] array = (ByteBuffer[])vals;
        final ByteBuffer[] ret = Arrays.copyOf(array, array.length+1);
        if(pos < array.length)
        {
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = newValue;
        return ret;
    }

    @Override
    public ByteBuffer[] valueArrayUpdateVal(Object vals, int pos, ByteBuffer newValue)
    {
        checkValidBuffer(newValue);
        ByteBuffer[] vals2 = (ByteBuffer[]) vals;
        vals2 = vals2.clone();
        vals2[pos] = newValue;
        return vals2;
    }

    @Override
    public ByteBuffer[] valueArrayFromArray(Object[] objects)
    {
        ByteBuffer[] ret = new ByteBuffer[objects.length];
        for(int i = 0; i < ret.length; ++i)
        {
            ret[i] = (ByteBuffer) objects[i];
        }
        return ret;
    }

    @Override
    public ByteBuffer[] valueArrayCopyOfRange(Object vals, int from, int to)
    {
        return Arrays.copyOfRange((ByteBuffer[])vals, from, to);
    }

    @Override
    public ByteBuffer[] valueArrayDeleteValue(Object vals, int pos)
    {
        ByteBuffer[] vals2 = new ByteBuffer[((ByteBuffer[])vals).length-1];
        System.arraycopy(vals,0, vals2, 0, pos-1);
        System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
        return vals2;
    }

    @Override
    public ByteBuffer nextValue(ByteBuffer value)
    {
        //value = value.duplicate();
        checkValidBuffer(value);
        byte[] ret = Arrays.copyOf(value.array(), value.limit());

        for (int i = ret.length-1; ; --i)
        {
            int b1 = ret[i] & 0xFF;
            if(b1 == 255)
            {
                if(i == 0)
                {
                    return null;
                }
                ret[i] = 0;
                {
                    continue;
                }
            }
            ret[i] = (byte) ((b1 + 1) & 0xFF);
            return ByteBuffer.wrap(ret);
        }
    }

    private void checkValidBuffer(ByteBuffer buffer)
    {
        if (buffer == null || buffer.isDirect())
        {
            throw new InvalidArgumentException("buffer must be non-null and non-direct");
        }
    }
}
