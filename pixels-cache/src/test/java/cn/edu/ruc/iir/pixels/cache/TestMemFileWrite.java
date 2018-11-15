package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

import java.io.File;
import java.util.Random;

public class TestMemFileWrite
{
    @Test
    public void test () throws Exception
    {
        String path = "/Users/Jelly/Desktop/pixels.index";
        new File(path);
        long [] addr = new long[1024*1024*32];
        Random random = new Random(System.nanoTime());
        for (int i = 0; i < 1024*1024*32; ++i)
        {
            addr[i] = random.nextInt(1024*1024*32);
        }

        long start = System.nanoTime();
        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L*1024L*10L);
        System.out.println((System.nanoTime()-start)/1000000.0);

        start = System.nanoTime();
        byte[] bytes = {0, 0, 0, 0, 0, 0, 0, 1};
        for (int i = 0; i < 1024*1024; ++i)
        {
            //mem.getAndAddLong(addr[i], 8);
            mem.putBytes(i * 8, bytes);
//            System.out.println(mem.getLong(addr[i]*8));
        }
        System.out.println("ns/op: " + (System.nanoTime()-start)/1024.0/1024/32);
    }
}
