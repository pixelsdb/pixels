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
package io.pixelsdb.pixels.cache;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * @create 2020-01-01
 * @author hank
 */
public class TestDirectByteBuffer
{
    @Test
    public void test ()
    {
        byte[] buffer = new byte[1024*1024*1024];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);//ByteBuffer.allocateDirect(1024*1024*1024);
        Random random = new Random();
        long start = System.nanoTime();
        for (int i = 0; i < 1024*1024; ++i)
        {
            //long vlong = byteBuffer.getLong(i*1000);
            long vlong = buffer[i*1000];
        }
        System.out.println((System.nanoTime()-start)/1000.0);
    }
}
