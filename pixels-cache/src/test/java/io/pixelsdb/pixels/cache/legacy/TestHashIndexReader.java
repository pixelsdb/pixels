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
package io.pixelsdb.pixels.cache.legacy;

import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TestHashIndexReader
{

    @Test
    public void testConstructor()
    {
        try
        {
            MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.hash-index", 102400000);
            HashIndexReader reader = new HashIndexReader(indexFile);
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    @Test
    public void testDiskIndexReader()
    {
        try
        {
            MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.hash-index", 102400000);
            RandomAccessFile indexDiskFile = new RandomAccessFile("/dev/shm/pixels.hash-index", "r");

            // read some chars
            byte[] buf = new byte[5];
            ByteBuffer wrapBuf = ByteBuffer.wrap(buf);
            indexDiskFile.seek(24);
            indexDiskFile.read(buf);
            Charset utf8 = StandardCharsets.UTF_8;
            System.out.println(utf8.decode(wrapBuf));
            for (int i = 0; i < buf.length; ++i)
            {
                System.out.println(buf[i]);
            }

            indexFile.getBytes(24, buf, 0, 5);
            wrapBuf.position(0);
            System.out.println(utf8.decode(wrapBuf));

            // print the byte value
            for (int i = 0; i < buf.length; ++i)
            {
                System.out.println(buf[i]);
            }

            // try to read the int
            // Note: the RandomAccessFile read everything from big-endian
            //      while mmap default read little-endian
            System.out.println("mmap " + indexFile.getInt(16));
            indexDiskFile.seek(16);
            System.out.println("disk " + indexDiskFile.readInt());

            System.out.println("mmap " + indexFile.getLong(16));
            indexDiskFile.seek(16);
            System.out.println("disk " + indexDiskFile.readLong());

        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testNativeSearch()
    {
        try
        {
            MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.hash-index", 102400000);
            HashIndexReader reader = new HashIndexReader(indexFile);
            NativeHashIndexReader nativeReader = new NativeHashIndexReader(indexFile);
            // blk=1073747693, rg=12, col=518
            System.out.println(reader.read(1073747693L, (short) 12, (short) 518));
            System.out.println(nativeReader.read(1073747693L, (short) 12, (short) 518));
            System.out.println(nativeReader.read(1073747647L, (short) 27, (short) 694));
            System.out.println(nativeReader.read(1073747600L, (short) 10, (short) 1013));


        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
