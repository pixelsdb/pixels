package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

/**
 * Created at: 18-10-24
 * Author: hank
 */
public class TestMemFileConcurrentRW
{
    @Test
    public void testRead () throws Exception
    {
        long start = System.nanoTime();
        MemoryMappedFile mem = new MemoryMappedFile("/dev/shm/test", 1024L*1024L*256L);
        System.out.println((System.nanoTime()-start)/1000000.0);
        while (true)
        {
            System.out.println(mem.getLong(16));
            Thread.sleep(1000);
        }
    }

    @Test
    public void testWrite () throws Exception
    {
        long start = System.nanoTime();
        MemoryMappedFile mem = new MemoryMappedFile("/dev/shm/test", 1024L*1024L*256L);
        System.out.println((System.nanoTime()-start)/1000000.0);
        while (true)
        {
            mem.getAndAddLong(16, 1);
            Thread.sleep(2000);
        }
    }
}
