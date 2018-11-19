package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class TestMemFileRead
{
    @Test
    public void test () throws Exception
    {
        String path = "/Users/Jelly/Desktop/pixels.index";

        long [] addr = new long[1024*1024];
        Random random = new Random(System.nanoTime());
        for (int i = 0; i < 1024*1024; ++i)
        {
            addr[i] = random.nextInt(1024*1024);
        }

        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L*1024L*10L);
        byte[] res = new byte[8];
        mem.getBytes(0, res, 0, 8);
        long v = ByteBuffer.wrap(res).order(ByteOrder.LITTLE_ENDIAN).getLong();
        System.out.println(v);
        for (int i = 0; i < 1024; ++i)
        {
            System.out.println(mem.getLong(addr[i] * 8));
        }
    }
}
