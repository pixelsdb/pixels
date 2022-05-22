package io.pixelsdb.pixels.cache;

import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class BenchmarkCacheContentReader {
//    List<PixelsCacheIdx> pixelsCacheIdxs = new ArrayList<>(4096);


    @Before
    public void prepare() throws IOException {
//        BufferedReader br = new BufferedReader(new FileReader("tmp.txt"));
//        String line = br.readLine();
//        String idxString = "";
//        while (line != null) {
//            idxString = line.split(";")[2];
//
//            String[] idxTokens = idxString.split("-");
//            long offset = Long.parseLong(idxTokens[0]);
//            int length = Integer.parseInt(idxTokens[1]);
//            pixelsCacheIdxs.add(new PixelsCacheIdx(offset, length));
//            line = br.readLine();
//        }
    }

    @Test
    public void benchmarkMultiThreadDiskContentReader() throws IOException, InterruptedException {
        int threadNum = 1;
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; ++i) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        List<PixelsCacheIdx> pixelsCacheIdxs = new ArrayList<>(4096);
                        BufferedReader br = new BufferedReader(new FileReader("tmp.txt"));
                        String line = br.readLine();
                        String idxString = "";
                        while (line != null) {
                            idxString = line.split(";")[2];

                            String[] idxTokens = idxString.split("-");
                            long offset = Long.parseLong(idxTokens[0]);
                            int length = Integer.parseInt(idxTokens[1]);
                            pixelsCacheIdxs.add(new PixelsCacheIdx(offset, length));
                            line = br.readLine();
                        }
                        CacheContentReader reader = new DiskCacheContentReader("/mnt/nvme1n1/pixels.cache");
                        Collections.shuffle(pixelsCacheIdxs);
                        long totalBytes = 0;
                        int maxBytes = 0;
                        for (PixelsCacheIdx idx : pixelsCacheIdxs) {
                            totalBytes += idx.length;
                            if (idx.length > maxBytes) maxBytes = idx.length;
                        }
                        byte[] interal = new byte[maxBytes];
                        ByteBuffer buf = ByteBuffer.wrap(interal);
                        long searchStart = System.nanoTime();
                        for (PixelsCacheIdx idx : pixelsCacheIdxs) {
                            reader.read(idx, buf);
                        }
                        long searchEnd = System.nanoTime();
                        double elapsed = (double) (searchEnd - searchStart) / (double) 1e6;
                        System.out.println("elapsed=" + elapsed + "ms" + " IOPS=" + pixelsCacheIdxs.size() / (elapsed / 1e3)
                                + " bandwidth=" + (totalBytes / 1000.0 / 1000.0) / (elapsed / 1e3) + "Mb/s" );

                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            });
        }

        for (int i = 0; i < threadNum; ++i) threads[i].start();
        for (int i = 0; i < threadNum; ++i) threads[i].join();


    }

    @Test
    public void benchmarkDiskContentReader() throws IOException {
        List<PixelsCacheIdx> pixelsCacheIdxs = new ArrayList<>(4096);
        BufferedReader br = new BufferedReader(new FileReader("tmp.txt"));
        String line = br.readLine();
        String idxString = "";
        while (line != null) {
            idxString = line.split(";")[2];

            String[] idxTokens = idxString.split("-");
            long offset = Long.parseLong(idxTokens[0]);
            int length = Integer.parseInt(idxTokens[1]);
            pixelsCacheIdxs.add(new PixelsCacheIdx(offset, length));
            line = br.readLine();
        }
        br.close();

        CacheContentReader reader = new DiskCacheContentReader("/mnt/nvme1n1/pixels.cache");
        Collections.shuffle(pixelsCacheIdxs);
        long totalBytes = 0;
        int maxBytes = 0;
        for (PixelsCacheIdx idx : pixelsCacheIdxs) {
            totalBytes += idx.length;
            if (idx.length > maxBytes) maxBytes = idx.length;
        }
        byte[] interal = new byte[maxBytes];
        ByteBuffer buf = ByteBuffer.wrap(interal);
        long searchStart = System.nanoTime();
        for (PixelsCacheIdx idx : pixelsCacheIdxs) {
            reader.read(idx, buf);
        }
        long searchEnd = System.nanoTime();
        double elapsed = (double) (searchEnd - searchStart) / (double) 1e6;
        System.out.println("elapsed=" + elapsed + "ms" + " IOPS=" + pixelsCacheIdxs.size() / (elapsed / 1e3)
                + " bandwidth=" + (totalBytes / 1000.0 / 1000.0) / (elapsed / 1e3) + "Mb/s" );

    }

}
