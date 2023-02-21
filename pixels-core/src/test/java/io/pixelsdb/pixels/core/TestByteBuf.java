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
package io.pixelsdb.pixels.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.pixelsdb.pixels.common.physical.natives.DirectIoLib;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class TestByteBuf
{
    @Test
    public void testEndian() throws IllegalAccessException, InvocationTargetException, IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        System.out.println(buffer.order());
        ByteBuffer buffer1 = DirectIoLib.allocateBuffer(10).getBuffer();
        System.out.println(buffer1.order());
        assert buffer1.order() == buffer.order();
    }

    @Test
    public void testByteReference()
    {
        byte[] content = new byte[2];
        content[0] = 5;
        content[1] = 8;
        process(content);
        System.out.println(content[0]);
    }

    public void process(byte[] content)
    {
        int index = 0;
        for (byte c : content)
        {
            content[index++] = (byte) (c + 1);
        }
    }

    @Test
    public void testByteBufferSlice()
    {
        ByteBuffer origin = ByteBuffer.allocate(100);
        for (int i = 0; i < 100; i++)
        {
            origin.put((byte) i);
        }
        origin.position(10);
        origin.limit(19);
        ByteBuffer slice = origin.slice();
        System.out.println(slice.arrayOffset());
        for (int i = 0; i < slice.capacity(); i++)
        {
            System.out.println(slice.get());
        }
        System.out.println(slice.position());
    }

    @Test
    public void testAllocation()
    {
        long start = System.currentTimeMillis();
        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        for (int i = 0; i < 10000; i++)
        {
            ByteBuf byteBuf = allocator.directBuffer(10000);
            byteBuf.release();
        }
        long end = System.currentTimeMillis();
        System.out.println("Elapsing time: " + (end - start));
    }

    @Test
    public void testNio()
    {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++)
        {
            ByteBuffer buffer = ByteBuffer.allocate(10000);
            buffer = null;
        }
        long end = System.currentTimeMillis();
        System.out.println("Elapsing time: " + (end - start));
    }

    @Test
    public void testStringBytes()
    {
        String s = "123456";
        char[] sChars = s.toCharArray();
        System.out.println(s.getBytes().length);
        System.out.println(sChars.length * Character.BYTES);
    }

    @Test
    public void test()
    {
        byte[] input = new byte[100000];
        long bufferStart = System.currentTimeMillis();
        ByteBuffer buffer = ByteBuffer.wrap(input);
        long bufferEnd = System.currentTimeMillis();
        System.out.println("ByteBuffer wrap cost: " + (bufferEnd - bufferStart));
        int sum = 0;
        long bufferReadStart = System.nanoTime();
        for (int i = 0; i < 10000; i++)
        {
            sum += buffer.getInt();
        }
        long bufferReadEnd = System.nanoTime();
        System.out.println("ByteBuffer read cost: " + (bufferReadEnd - bufferReadStart) + ", sum: " + sum);

        long bufStart = System.currentTimeMillis();
        ByteBuf buf = Unpooled.wrappedBuffer(input);
        long bufEnd = System.currentTimeMillis();
        System.out.println("ByteBuf wrap cost: " + (bufEnd - bufStart));
        sum = 0;
        long bufReadStart = System.nanoTime();
        for (int i = 0; i < 10000; i++)
        {
            sum += buf.readInt();
        }
        long bufReadEnd = System.nanoTime();
        System.out.println("ByteBuf read cost: " + (bufReadEnd - bufReadStart) + ", sum: " + sum);
    }

    @Test
    public void testReference()
    {
        String a = "aaa";
        String b = a;
        System.out.println(a);
        System.out.println(b);
        a = null;
        System.out.println(a);
        System.out.println(b);
    }

    @Test
    public void testStringNullPointerException()
    {
        byte[] content = new byte[10];
        content[0] = 1;
        String v = new String(content, 0, 0);
        System.out.println(v);
    }
}

