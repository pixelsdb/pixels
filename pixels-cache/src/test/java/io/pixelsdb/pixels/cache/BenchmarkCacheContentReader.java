package io.pixelsdb.pixels.cache;

import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

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

    static class BenchmarkResult {
        double elapsed;
        double totalBytes;
        double totalIO;
        double iops;
        double bandwidthMb;
        double bandwidthMib;

        double latency;

        BenchmarkResult(double totalIO, double totalBytes, double elapsedInMili) {
            this.elapsed = elapsedInMili;
            this.totalBytes = totalBytes;
            this.totalIO = totalIO;
            this.iops = totalIO / (elapsed / 1e3);
            this.bandwidthMb = (totalBytes / 1000.0 / 1000.0) / (elapsed / 1e3);
            this.bandwidthMib = (totalBytes / 1024.0 / 1024.0) / (elapsed / 1e3);
            this.latency = 1.0 / this.iops * 1000;
        }

        @Override
        public String toString() {
            return String.format("elapsed=%fms(%fs), IOPS=%f, bandwidth=%fMB/s(%fMiB/s), latency=%fms, totalIO=%f, totalBytes=%fGiB",
                    elapsed, elapsed / 1e3, iops, bandwidthMb, bandwidthMib, latency, totalIO, totalBytes / 1024.0 / 1024.0 / 1024.0);
        }

    }

    private void benchmarkMultiThreadContentReader(Supplier<CacheContentReader> factory, int threadNum) throws InterruptedException, ExecutionException {
        ExecutorService[] executors = new ExecutorService[threadNum];
        List<Future<BenchmarkResult>> futures = new ArrayList<>();

        for (int i = 0; i < threadNum; ++i) {
            executors[i] = Executors.newSingleThreadExecutor();
            Future<BenchmarkResult> future = executors[i].submit(() -> {
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
//                        CacheContentReader reader = new DiskCacheContentReader("/mnt/nvme1n1/pixels.cache");
                    CacheContentReader reader = factory.get();

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
                    BenchmarkResult result = new BenchmarkResult(pixelsCacheIdxs.size(), totalBytes, elapsed);
                    System.out.println(result);
//                        System.out.println("elapsed=" + elapsed + "ms" + " IOPS=" + pixelsCacheIdxs.size() / (elapsed / 1e3)
//                                + " bandwidth=" + (totalBytes / 1000.0 / 1000.0) / (elapsed / 1e3) + "Mb/s" + " readCount=" + pixelsCacheIdxs.size());
                    return result;

                } catch (IOException e) {
                    e.printStackTrace();
                }

                return null;
            });

            futures.add(future);

        }

        List<BenchmarkResult> results = new ArrayList<>(threadNum);
        for (int i = 0; i < threadNum; ++i) {
            results.add(futures.get(i).get());
        }
        double totalIOPS = 0.0;
        double totalBandwidthMB = 0.0;
        double totalBandwidthMiB = 0.0;

        double averageLatency = 0.0;
        for (BenchmarkResult res : results) {
            totalIOPS += res.iops;
            totalBandwidthMB += res.bandwidthMb;
            totalBandwidthMiB += res.bandwidthMib;
            averageLatency += res.latency;
        }
        averageLatency /= threadNum;
        System.out.println(String.format("threads=%d, totalIOPS=%f, bandwidth=%fMB(%fMiB), latency=%fms", threadNum, totalIOPS, totalBandwidthMB, totalBandwidthMiB, averageLatency));

    }

    @Test
    public void benchmarkMultiThreadDiskContentReader() throws IOException, InterruptedException, ExecutionException {
        int threadNum = 8;
        benchmarkMultiThreadContentReader(() -> {
            try {
                return new DiskCacheContentReader("/scratch/yeeef/pixels-cache/pixels.cache");
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }, threadNum);
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
