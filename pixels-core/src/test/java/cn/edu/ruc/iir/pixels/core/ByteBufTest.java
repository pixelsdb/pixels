package cn.edu.ruc.iir.pixels.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class ByteBufTest
{
    @Test
    public void testAllocation()
    {
        long start = System.currentTimeMillis();
        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        for (int i = 0; i < 10000; i++)
        {
            ByteBuf byteBuf = allocator.directBuffer(10000);
            byteBuf.release();
        }
        long end = System.currentTimeMillis();
        System.out.println("Elapsing time: " + (end - start));
    }

    @Test
    public void testNio()
    {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++)
        {
            ByteBuffer buffer = ByteBuffer.allocate(10000);
            buffer = null;
        }
        long end = System.currentTimeMillis();
        System.out.println("Elapsing time: " + (end - start));
    }
}
