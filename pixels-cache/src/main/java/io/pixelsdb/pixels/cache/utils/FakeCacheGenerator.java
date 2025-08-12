/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.cache.utils;

import io.pixelsdb.pixels.cache.PixelsCacheIdx;
import io.pixelsdb.pixels.cache.PixelsCacheKey;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// generate fake cache data based on the cache index file
public class FakeCacheGenerator
{
    List<PixelsCacheKey> pixelsCacheKeys;
    List<PixelsCacheIdx> pixelsCacheIdxs;

    FakeCacheGenerator(String serializeFileName) throws IOException
    {
        pixelsCacheKeys = new ArrayList<>();
        pixelsCacheIdxs = new ArrayList<>();

        BufferedReader br = new BufferedReader(new FileReader(serializeFileName));
        String line = br.readLine();
        String keyString = "";
        String idxString = "";
        while (line != null)
        {
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
    }

    public static void main(String[] args) throws IOException
    {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        FakeCacheGenerator gen = new FakeCacheGenerator("pixels-cache/dumpedCache.txt");
        gen.gen("/mnt/nvme1n1/pixels.cache");
    }

    // gen as a contigous file
    public void gen(String outputName) throws IOException
    {
        RandomAccessFile out = new RandomAccessFile(outputName, "rw");
        int fakeIdx = 0;
        byte[] writeBuf = new byte[4096];
        for (int i = 0; i < pixelsCacheIdxs.size(); ++i)
        {
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
    public void genFiles(String baseDir) throws IOException
    {
        int fakeIdx = 0;
        byte[] writeBuf = new byte[4096];
        for (PixelsCacheIdx idx : pixelsCacheIdxs)
        {
            Path fileLoc = Paths.get(baseDir, idx.offset + "-" + idx.length);
            RandomAccessFile out = new RandomAccessFile(fileLoc.toString(), "rw");
            char fakeValue = (char) ('A' + fakeIdx);
            if (idx.length > writeBuf.length) writeBuf = new byte[idx.length];
            Arrays.fill(writeBuf, 0, idx.length, (byte) fakeValue);
            out.write(writeBuf, 0, idx.length);
            out.close();
            fakeIdx = (fakeIdx + 1) % 26;
        }

    }
}
