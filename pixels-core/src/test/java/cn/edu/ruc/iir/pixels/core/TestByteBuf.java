package cn.edu.ruc.iir.pixels.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
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
        byte[] input = new byte[100000];
        long bufferStart = System.currentTimeMillis();
        ByteBuffer buffer = ByteBuffer.wrap(input);
        long bufferEnd = System.currentTimeMillis();
        System.out.println("ByteBuffer wrap cost: " + (bufferEnd - bufferStart));
        int sum = 0;
        long bufferReadStart = System.nanoTime();
        for (int i = 0; i < 10000; i++)
        {
            sum += buffer.getInt();
        }
        long bufferReadEnd = System.nanoTime();
        System.out.println("ByteBuffer read cost: " + (bufferReadEnd - bufferReadStart) + ", sum: " + sum);

        long bufStart = System.currentTimeMillis();
        ByteBuf buf = Unpooled.wrappedBuffer(input);
        long bufEnd = System.currentTimeMillis();
        System.out.println("ByteBuf wrap cost: " + (bufEnd - bufStart));
        sum = 0;
        long bufReadStart = System.nanoTime();
        for (int i = 0; i < 10000; i++)
        {
            sum += buf.readInt();
        }
        long bufReadEnd = System.nanoTime();
        System.out.println("ByteBuf read cost: " + (bufReadEnd - bufReadStart) + ", sum: " + sum);
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

