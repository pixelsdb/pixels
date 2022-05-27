package io.pixelsdb.pixels.cache;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// generate fake cache data based on the cache index
public class FakeCacheGenerator {
    List<PixelsCacheKey> pixelsCacheKeys;
    List<PixelsCacheIdx> pixelsCacheIdxs;


    FakeCacheGenerator(String serializeFileName) throws IOException {
        pixelsCacheKeys = new ArrayList<>();
        pixelsCacheIdxs = new ArrayList<>();

        BufferedReader br = new BufferedReader(new FileReader(serializeFileName));
        String line = br.readLine();
        String keyString = "";
        String idxString = "";
        while (line != null) {
            keyString = line.split(";")[1];
            idxString = line.split(";")[2];
            String[] keyTokens = keyString.split("-");
            long blockId = Long.parseLong(keyTokens[0]);
            short rowGroupId = Short.parseShort(keyTokens[1]);
            short columnId = Short.parseShort(keyTokens[2]);

            String[] idxTokens = idxString.split("-");
            long offset = Long.parseLong(idxTokens[0]);
            int length = Integer.parseInt(idxTokens[1]);
            pixelsCacheKeys.add(new PixelsCacheKey(blockId, rowGroupId, columnId));
            pixelsCacheIdxs.add(new PixelsCacheIdx(offset, length));

            line = br.readLine();
        }
        System.out.println(pixelsCacheKeys.subList(0, 10));
        System.out.println(pixelsCacheIdxs.subList(0, 10));



//        System.out.println(Arrays.toString(Arrays.copyOfRange(pixelsCacheKeys, 0, 10)));
    }

    // gen as a contigous file
    public void gen(String outputName) throws IOException {
        RandomAccessFile out = new RandomAccessFile(outputName, "rw");
        int fakeIdx = 0;
        byte[] writeBuf = new byte[4096];
        for (int i = 0; i < pixelsCacheIdxs.size(); ++i) {
            char fakeValue = (char) ('A' + fakeIdx);
            PixelsCacheIdx idx = pixelsCacheIdxs.get(i);
            out.seek(idx.offset);
            if (idx.length > writeBuf.length) writeBuf = new byte[idx.length];
            Arrays.fill(writeBuf, 0, idx.length, (byte) fakeValue);
            out.write(writeBuf, 0, idx.length);
            fakeIdx = (fakeIdx + 1) % 26;
        }

        out.close();
    }

    // gen to different files
    public void genFiles(String baseDir) throws IOException {
        int fakeIdx = 0;
        byte[] writeBuf = new byte[4096];
        for (PixelsCacheIdx idx : pixelsCacheIdxs) {
            Path fileLoc = Paths.get(baseDir, String.valueOf(idx.offset) + "-" + String.valueOf(idx.length));
            RandomAccessFile out = new RandomAccessFile(fileLoc.toString(), "rw");
            char fakeValue = (char) ('A' + fakeIdx);
            if (idx.length > writeBuf.length) writeBuf = new byte[idx.length];
            Arrays.fill(writeBuf, 0, idx.length, (byte) fakeValue);
            out.write(writeBuf, 0, idx.length);
            out.close();
            fakeIdx = (fakeIdx + 1) % 26;
        }

    }

    public static void main(String[] args) throws IOException {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        FakeCacheGenerator gen = new FakeCacheGenerator("pixels-cache/tmp.txt");
//        gen.gen("/mnt/nvme1n1/pixels.cache");
//        gen.gen("/scratch/yeeef/pixels-cache/pixels.cache");
        gen.genFiles("/scratch/yeeef/pixels-cache/cache_fs");

    }



}
