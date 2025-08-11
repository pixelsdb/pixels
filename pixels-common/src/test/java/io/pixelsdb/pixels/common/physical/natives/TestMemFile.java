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
package io.pixelsdb.pixels.common.physical.natives;

import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author tao
 * @create 2019-02-21 16:41
 **/
public class TestMemFile
{
    String path = "/dev/shm/pixels.cache";

    @Test
    public void testRound4096()
    {
        assert (MemoryMappedFile.roundTo4096(0) == 0);
        assert (MemoryMappedFile.roundTo4096(1) == 4096);
        assert (MemoryMappedFile.roundTo4096(4097) == 8192);
        assert (MemoryMappedFile.roundTo4096(4096 * 37 + 2048) == 4096 * 38);
        assert (MemoryMappedFile.roundTo4096(4096 * 37 - 1453) == 4096 * 37);
    }

    @Test
    public void testWrite() throws Exception
    {
        MemoryMappedFile file = new MemoryMappedFile(this.path, 4L * 1024 * 1024 * 1024);
        file.setInt(2 * 1024 * 1024, 1);
        System.out.println(file.getInt(2 * 1024 * 1024));
        file.unmap();
    }

    @Test
    public void testRead() throws Exception
    {
        MemoryMappedFile file = new MemoryMappedFile(this.path, 4L * 1024 * 1024 * 1024);
        System.out.println(file.getInt(2 * 1024 * 1024));
        file.unmap();
    }

    @Test
    public void testEndian () throws Exception
    {
        write(ByteOrder.BIG_ENDIAN, 0xf0ff00008fff0000L);
        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);
        byte[] bytes = new byte[8];
        mem.getBytes(0, bytes, 0, 8);
        for (int i = 0; i < 8; ++i)
        {
            System.out.println(bytes[i]);
        }
        long v = mem.getLongVolatile(0);
        System.out.println(v);
    }

    @Test
    public void testOpenMemFile() throws Exception
    {
        long start = System.nanoTime();
        for (int i = 0; i < 100; ++i)
        {
            MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L);
        }
        long duration = System.nanoTime() - start;
        System.out.println((duration/1000) + " us");
    }

    @Test
    public void testMulti() throws Exception
    {
        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);
        Map<Integer, byte[]> kvMap = new HashMap<>();
        write(kvMap);

        long startReadTime = System.currentTimeMillis();
        Reader reader[] = new Reader[64];
        int r_num = 50;
        for (int i = 0; i < r_num; i++)
        {
            reader[i] = new Reader(mem, kvMap);
            reader[i].start();
        }
        for (int i = 0; i < r_num; i++)
        {
            try
            {
                reader[i].join();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        long endReadTime = System.currentTimeMillis();
        System.out.println("Read cost time: " + (endReadTime - startReadTime));
    }

    private void write(Map<Integer, byte[]> kvMap) throws Exception
    {
        new File(path);

        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);

        byte[] bytes = new byte[8];
        for (int i = 0; i < 1024; ++i)
        {
            bytes = randomByte(new Random(), 8);
            kvMap.put(i, bytes);
            mem.setBytes(8 * i, bytes);
        }
    }

    public static byte[] randomByte(Random random, int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++)
        {
            bytes[i] = (byte) (random.nextInt(10) + 48);
        }
        return bytes;
    }

    @Test
    public void test() throws Exception
    {
        write(ByteOrder.BIG_ENDIAN, 0xffff0000ffff0000L);
        read();
    }

    public void write(ByteOrder byteOrder, long repeatLong) throws Exception
    {
        new File(path);

        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);

        ByteBuffer buffer = ByteBuffer.allocate(8).order(byteOrder);
        buffer.putLong(repeatLong);
        byte[] bytes = buffer.array();
        for (int i = 0; i < 1024; ++i)
        {
            //mem.putBytes(8 * i, bytes);
            mem.setLong(8 * i, repeatLong);
        }
    }

    public void read() throws Exception
    {
        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);
        byte[] res = new byte[8];
        mem.getBytes(0, res, 0, 8);
        long v = ByteBuffer.wrap(res).order(ByteOrder.LITTLE_ENDIAN).getLong();
        System.out.println(v);
        for (int i = 0; i < 10; ++i)
        {
            System.out.println(mem.getLong(i * 8));
        }
    }

    class Reader extends Thread
    {
        ThreadLocal<byte[]> localValue = ThreadLocal.withInitial(() -> new byte[8]);
        MemoryMappedFile mem;
        Map<Integer, byte[]> kvMap;

        public Reader(MemoryMappedFile mem, Map<Integer, byte[]> kvMap)
        {
            this.mem = mem;
            this.kvMap = kvMap;
        }

        @Override
        public void run()
        {
            byte[] value = localValue.get();
            Random random = new Random();
            int len = random.nextInt(1024);
            System.out.println(Thread.currentThread().getId() + "," + len + " start");
            for (int i = 0; i < len; ++i)
            {
                byte[] oldValue = kvMap.get(i);
                mem.getBytes(i * 8, value, 0, 8);

                if (!Arrays.equals(value, oldValue))
                {
                    try
                    {
                        System.out.println(
                                "ERROR in read: inconsistent value\n" + Arrays.toString(value) + "\n" + Arrays.toString(oldValue));
                        System.exit(-1);
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }

                }
            }
            System.out.println(Thread.currentThread().getName() + "," + len + " end");
        }
    }
}

