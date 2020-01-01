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
package io.pixelsdb.pixels.test;

import io.pixelsdb.pixels.cache.MemoryMappedFile;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;

/**
 * <file> <file size> <access size> <parallelism> <round>
 * java -jar pixels-cache-0.1.0-SNAPSHOT-full.jar /dev/shm/pixels.cache 17179869184 4096 20 100000
 *
 * @author hank
 */

public class MemMappedFilePerf
{
    public static void main(String[] args)
    {
        String path = args[0];
        long fileSize = Long.parseLong(args[1]);
        int acSize = Integer.parseInt(args[2]);
        int parallelism = Integer.parseInt(args[3]);
        int round = Integer.parseInt(args[4]);

        try
        {
            MemoryMappedFile mappedFile = new MemoryMappedFile(path, fileSize);
            Thread[] readers = new Thread[parallelism];
            long begin = System.nanoTime();
            for (int i = 0; i < parallelism; i++)
            {
                Reader reader = new Reader(mappedFile, i, fileSize, acSize, round);
                Thread readerT = new Thread(reader);
                readers[i] = readerT;
                readerT.start();
            }
            for (int i = 0; i < parallelism; i++)
            {
                readers[i].join();
            }
            long end = System.nanoTime();
            long cost = end - begin;
            System.out.println("Total op/ms " + (parallelism * round * 1.0d / (cost * 1.0d / 1000000.0d)));
            System.out
                    .println("Total MB/ms " + (parallelism * round / 1024.0d * acSize / 1024.0d / (cost / 1000000.0d)));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    static class Reader
            implements Runnable
    {
        MemoryMappedFile mappedFile;
        int id;
        long fileSize;
        int acSize;
        int round;

        public Reader(MemoryMappedFile mappedFile, int id, long fileSize, int acSize, int round)
        {
            this.mappedFile = mappedFile;
            this.id = id;
            this.fileSize = fileSize;
            this.acSize = acSize;
            this.round = round;
        }

        @Override
        public void run()
        {
            Random random = new Random(System.nanoTime());
            Iterator<Long> offsetItr = random.longs(0, (fileSize / acSize) - 1).iterator();
            long[] offsets = new long[round];
            for (int i = 0; i < round; i++)
            {
                offsets[i] = offsetItr.next() * acSize;
            }
            int count = 0;
            byte[] result = new byte[acSize];
            long begin = System.nanoTime();
            while (count < round)
            {
                //mappedFile.getBytes(offsets[count], result, 0, acSize);
                ByteBuffer byteBuffer = mappedFile.getDirectByteBuffer(offsets[count], acSize);
                /*for (int i = 0; i < acSize; ++i)
                {
                    byteBuffer.get(i);
                }*/
                byteBuffer.get(result);
                count++;
            }
            long end = System.nanoTime();
            long cost = end - begin;
            double readSize = round * 1.0d * acSize;
            System.out.println("Thread " + id + " op/ms " + (round * 1.0d / (cost / 1000000.0d)));
            System.out.println("Thread " + id + " MB/ms: " + readSize / 1024.0d / 1024.0d / (cost * 1.0d / 1000000.0d));
        }
    }
}
