/*
 * Copyright 2018 PixelsDB.
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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.cache;

import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestMemFileWrite
{
    @Test
    public void test()
            throws Exception
    {
        String path = "/Users/Jelly/Desktop/pixels.index";
        new File(path);

        long start = System.nanoTime();
        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);
        System.out.println((System.nanoTime() - start) / 1000000.0);

        start = System.nanoTime();
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(1);
        byte[] bytes = buffer.array();
        for (int i = 0; i < 1024 * 1024; ++i)
        {
            //mem.getAndAddLong(addr[i], 8);
//            mem.putBytes(i * 8, bytes);
            mem.putLong(i * 8, 1);
        }
        System.out.println("ns/op: " + (System.nanoTime() - start) / 1024.0 / 1024);
    }
}
