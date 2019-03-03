package cn.edu.ruc.iir.pixels.cache;

import java.util.Iterator;
import java.util.Random;

public class TestMemFileRead
{
    public static void main(String[] args)
    {
        String path = args[0];
        long size = Long.parseLong(args[1]);

        int[] acSizes = {10, 100, 1024, 10240, 102400, 204800, 409600, 614400, 819200, 1048576, 2097152, 4194304, 6291456,8388608, 10485760};  // 10, 100, 1KB, 10KB, 100KB, 1MB, 10MB

        try {
            MemoryMappedFile mappedFile = new MemoryMappedFile(path, size);
            Random random = new Random();
            long[][] acTimeMatrix = new long[acSizes.length][1000];
            for (int i = 0; i < acSizes.length; i++)
            {
                acTimeMatrix[i] = new long[1000];
                int acSize = acSizes[i];
                byte[] res = new byte[acSize];
                Iterator<Long> offsetItr = random.longs(0, size - acSize).iterator();
                for (int k = 0; k < 1000; k++)
                {
                    if (!offsetItr.hasNext()) {
                        offsetItr = random.longs(0, size - acSize).iterator();
                    }
                    long offset = offsetItr.next();
                    long startNano = System.nanoTime();
                    mappedFile.getBytes(offset, res, 0, acSize);
                    long endNano = System.nanoTime();
                    acTimeMatrix[i][k] = endNano - startNano;
                }
            }
            for (int i = 0; i < acSizes.length; i++)
            {
                int acSize = acSizes[i];
                long acTimeTotal = 0L;
                for (int k = 0; k < 1000; k++)
                {
                    acTimeTotal += acTimeMatrix[i][k];
                }
                double acTimeAvg = acTimeTotal * 1.0d / 1000.0d;
                System.out.println("Average access time for size " + acSize + " bytes: " + acTimeAvg + "ns");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
