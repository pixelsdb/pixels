package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author: tao
 * @date: Create in 2019-02-21 16:41
 **/
public class TestMemFile {
    String path = "/dev/shm/pixels.index";

    @Test
    public void testMulti() throws Exception {
        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);
        Map<Integer, byte[]> kvMap = new HashMap<>();
        write(kvMap);

        long startReadTime = System.currentTimeMillis();
        Reader reader[] = new Reader[64];
        int r_num = 50;
        for (int i = 0; i < r_num; i++) {
            reader[i] = new Reader(mem, kvMap);
            reader[i].start();
        }
        for (int i = 0; i < r_num; i++) {
            try {
                reader[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long endReadTime = System.currentTimeMillis();
        System.out.println("Read cost time: " + (endReadTime - startReadTime));
    }

    private void write(Map<Integer, byte[]> kvMap) throws Exception {
        new File(path);

        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);

        byte[] bytes = new byte[8];
        for (int i = 0; i < 1024; ++i) {
            bytes = randomByte(new Random(), 8);
            kvMap.put(i, bytes);
            mem.putBytes(8 * i, bytes);
        }
    }

    public static byte[] randomByte(Random random, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (random.nextInt(10) + 48);
        }
        return bytes;
    }

    @Test
    public void test() throws Exception {
        write();
        read();
    }

    public void write() throws Exception {
        new File(path);

        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);

        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(1);
        byte[] bytes = buffer.array();
        for (int i = 0; i < 1024; ++i) {
            mem.putBytes(8 * i, new byte[1]);
        }
    }

    public void read() throws Exception {
        MemoryMappedFile mem = new MemoryMappedFile(path, 1024L * 1024L * 10L);
        byte[] res = new byte[8];
        mem.getBytes(0, res, 0, 8);
        long v = ByteBuffer.wrap(res).order(ByteOrder.LITTLE_ENDIAN).getLong();
        System.out.println(v);
        for (int i = 0; i < 10; ++i) {
            System.out.println(mem.getLong(i * 8));
        }
    }

    class Reader extends Thread {
        ThreadLocal<byte[]> localValue = ThreadLocal.withInitial(() -> new byte[8]);
        MemoryMappedFile mem;
        Map<Integer, byte[]> kvMap;

        public Reader(MemoryMappedFile mem, Map<Integer, byte[]> kvMap) {
            this.mem = mem;
            this.kvMap = kvMap;
        }

        @Override
        public void run() {
            byte[] value = localValue.get();
            Random random = new Random();
            int len = random.nextInt(1024);
            System.out.println(Thread.currentThread().getId() + "," + len + " start");
            for (int i = 0; i < len; ++i) {
                byte[] oldValue = kvMap.get(i);
                mem.getBytes(i * 8, value, 0, 8);

                if (!Arrays.equals(value, oldValue)) {
                    try {
                        System.out.println("ERROR in read: 值不相同\n" + Arrays.toString(value) + "\n" + Arrays.toString(oldValue));
                        System.exit(-1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
            System.out.println(Thread.currentThread().getName() + "," + len + " end");
        }
    }

}

