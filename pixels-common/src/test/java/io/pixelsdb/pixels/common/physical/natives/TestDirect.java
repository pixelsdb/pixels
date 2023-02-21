/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.common.physical.natives;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

/**
 * Created at: 02/02/2023
 * Author: hank
 */
public class TestDirect
{
    @Test
    public void testDirectIoLib() throws IOException, IllegalAccessException, InvocationTargetException
    {
        int fd = DirectIoLib.open("/home/hank/20230126155625_0.pxl", true);
        DirectBuffer buffer = DirectIoLib.allocateBuffer(8);
        int read = DirectIoLib.read(fd, 4094, buffer, 8);
        System.out.println(read);
        for (int i = 0; i < 8; ++i)
        {
            System.out.println(buffer.get());
        }
    }

    @Test
    public void testMemoryAllocation() throws InvocationTargetException, IllegalAccessException, IOException
    {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; ++i)
        {
            ByteBuffer buffer = ByteBuffer.allocateDirect(1);
            DirectBuffer directBuffer = new DirectBuffer(buffer, buffer.capacity(), true);
            directBuffer.close();
        }
        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; ++i)
        {
            ByteBuffer buffer = ByteBuffer.allocateDirect(1);
            DirectBuffer directBuffer = new DirectBuffer(buffer, buffer.capacity(), true);
            directBuffer.close();
        }
        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; ++i)
        {
            DirectBuffer buffer = DirectIoLib.allocateBuffer(1);
            buffer.close();
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void testDirectRaf() throws IOException
    {
        DirectRandomAccessFile raf = new DirectRandomAccessFile(new File("/home/hank/20230126155625_0.pxl"), true);
        raf.seek(raf.length()-8);
        int a = raf.readInt();
        System.out.println(a);
        raf.seek(raf.length()-4);
        int b = raf.readInt();
        System.out.println(b);
        System.out.println(raf.length());
    }
}
