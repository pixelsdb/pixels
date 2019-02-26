package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.cache.MemoryMappedFile;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * pixels
 *
 * @author guodong
 */
public class TestMemConcurrent
{
    public static void main(String[] args)
    {
        try {
            MemoryMappedFile memoryMappedFile = new MemoryMappedFile("/Users/Jelly/Desktop/memfile", 1024 * 1024);
            long base = 0L;
            for (int i = 0; i < 10000; i++)
            {
                memoryMappedFile.putLongVolatile(base, i);
                base += 8;
            }
            for (int i = 0; i < 10000; i++)
            {
                memoryMappedFile.putIntVolatile(base, i);
                base += 4;
            }
            for (int i = 0; i < 10000; i++)
            {
                ByteBuffer buffer = ByteBuffer.allocate(4);
                buffer.putInt(i);
                memoryMappedFile.putBytes(base, buffer.array());
                base += 4;
            }

            Thread[] readers = new Thread[10];
            for (int i = 0; i < 10; i++)
            {
                Reader reader = new Reader(memoryMappedFile);
                Thread thread = new Thread(reader);
                thread.start();
                readers[i] = thread;
            }
            for (int i = 0; i < 10; i++)
            {
                readers[i].join();
            }
            memoryMappedFile.unmap();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Reader implements Runnable
    {
        private final MemoryMappedFile memoryMappedFile;

        public Reader(MemoryMappedFile memoryMappedFile)
        {
            this.memoryMappedFile = memoryMappedFile;
        }

        @Override
        public void run()
        {
            Random random = new Random(System.nanoTime());
            int counter = 0;
            while (counter < 8000)
            {
                int id = random.nextInt(10000);
                if (id < 0)
                {
                    continue;
                }
                long value = memoryMappedFile.getLong(id * 8);
                if (value != id)
                {
                    System.out.println("[error] get long not match " + value + ":" + id);
                }
//                else
//                {
//                    System.out.println("[ok] get long " + value);
//                }
                counter++;
            }
            counter = 0;
            int base = 10000 * 8;
            while (counter < 8000)
            {
                int id  = random.nextInt(10000);
                if (id < 0)
                {
                    continue;
                }
                int value = memoryMappedFile.getInt(id * 4 + base);
                if (value != id)
                {
                    System.out.println("[error] get int not match " + value + ":" + id);
                }
//                else
//                {
//                    System.out.println("[ok] get int " + value);
//                }
                counter++;
            }
            counter = 0;
            base = 10000 * 12;
            while (counter < 8000)
            {
                int id  = random.nextInt(10000);
                if (id < 0)
                {
                    continue;
                }
                byte[] data = new byte[4];
                memoryMappedFile.getBytes(base + (id * 4), data, 0, 4);
                counter++;
                ByteBuffer buffer = ByteBuffer.wrap(data);
                int value = buffer.getInt();
                if (value != id)
                {
                    System.out.println("[error] get bytes not match " + value + ":" + id);
                }
            }
        }
    }
}
