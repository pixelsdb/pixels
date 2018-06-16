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
public class TestByteBuf
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

    @Test
    public void testStringBytes()
    {
        String s = "123456";
        char[] sChars = s.toCharArray();
        System.out.println(s.getBytes().length);
        System.out.println(sChars.length * Character.BYTES);
    }

    @Test
    public void test()
    {
        byte[][] matrix = new byte[10][10];
        byte[] row = matrix[0];
        System.out.println(row[0]);
        matrix[0] = null;
        System.out.println(row[0]);
    }

    @Test
    public void testReference()
    {
        String a = "aaa";
        String b = a;
        System.out.println(a);
        System.out.println(b);
        a = null;
        System.out.println(a);
        System.out.println(b);
    }
}

