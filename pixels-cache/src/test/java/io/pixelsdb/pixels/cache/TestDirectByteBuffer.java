package io.pixelsdb.pixels.cache;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created at: 20-1-1
 * Author: hank
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
