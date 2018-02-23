package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

import java.io.File;

public class TestMemFileWrite
{
    @Test
    public void test () throws Exception
    {

        new File("/Users/Jelly/shm/test").delete();
        long start = System.nanoTime();
        MemoryMappedFile mem = new MemoryMappedFile("/Users/Jelly/shm/test", 1024L*1024L*56L);
        System.out.println((System.nanoTime()-start)/1000000.0);
        start = System.nanoTime();
        for (long i = 0; i < 1024L*1024L*7L; ++i)
        {//mem.getAndAddLong(0, 8);
            mem.putLong(i*8, i);
        }
        System.out.println("ns/op: " + (System.nanoTime()-start)/1024.0/1024/512);
    }
}
