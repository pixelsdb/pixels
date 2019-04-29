package cn.edu.ruc.iir.pixels.cache;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class TestMemFileWrite
{
    public static final long TEST_NUM = 100L;
    public static final long MEM_SIZE = 1024L*1000L*1000L;

    public static void main(String[] args) throws Exception
    {
        String path = "/dev/shm/test_shm";
        new File(path);

        long start = System.nanoTime();
        MemoryMappedFile mem = new MemoryMappedFile(path, MEM_SIZE);
        System.out.println((System.nanoTime()-start)/1000000.0);

        Random random = new Random(System.nanoTime());

        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(1);
        byte[] bytes = buffer.array();

        start = System.nanoTime();
        for (int i = 0; i < TEST_NUM; ++i)
        {
            //byte[] bytes = new byte[1000];
            long pos = random.nextLong() % MEM_SIZE;
            if (pos < 0)
            {
                pos = -pos;
            }
            if (pos > MEM_SIZE - 1000)
            {
                pos -= 1000;
            }
            //mem.getAndAddLong(pos, 8);
            //mem.getBytes(pos, bytes, 0, 1000);
            mem.putLong(pos, 1);
        }
        System.out.println("ns/op: " + (System.nanoTime()-start)*1.0/TEST_NUM);
    }
}
