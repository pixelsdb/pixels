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
package io.pixelsdb.pixels.cache.legacy;

import io.pixelsdb.pixels.cache.PixelsCacheIdx;
import io.pixelsdb.pixels.cache.PixelsCacheKey;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestIndexWriter
{
    List<PixelsCacheIdx> pixelsCacheIdxs = new ArrayList<>(4096);
    List<PixelsCacheKey> pixelsCacheKeys = new ArrayList<>(4096);

    @Before
    public void loadMockData() throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader("dumpedCache.txt"));
        String line = br.readLine();
        String idxString = "";
        String keyString = "";
        while (line != null)
        {
            keyString = line.split(";")[1];
            idxString = line.split(";")[2];

            String[] keyTokens = keyString.split("-");
            long blkId = Long.parseLong(keyTokens[0]);
            short rgId = Short.parseShort(keyTokens[1]);
            short colId = Short.parseShort(keyTokens[2]);

            String[] idxTokens = idxString.split("-");
            long offset = Long.parseLong(idxTokens[0]);
            int length = Integer.parseInt(idxTokens[1]);
            pixelsCacheIdxs.add(new PixelsCacheIdx(offset, length));
            pixelsCacheKeys.add(new PixelsCacheKey(blkId, rgId, colId));
            line = br.readLine();
        }
    }

    @Test
    public void testHashIndexWriter() throws Exception
    {
        Configurator.setRootLevel(Level.DEBUG);

        MemoryMappedFile hashIndex = new MemoryMappedFile("/dev/shm/pixels.hash-index-test", 512000 * 24 * 2);
        // write a new hash index
        CacheIndexWriter indexWriter = new HashIndexWriter(hashIndex);
        CacheIndexReader indexReader = new HashIndexReader(hashIndex);

        for (int i = 0; i < pixelsCacheIdxs.size(); ++i)
        {
            PixelsCacheKey cacheKey = pixelsCacheKeys.get(i);
            PixelsCacheIdx cacheIdx = pixelsCacheIdxs.get(i);
            indexWriter.put(cacheKey, cacheIdx);
            PixelsCacheIdx readed = indexReader.read(cacheKey);
            if (readed == null || readed.length != cacheIdx.length)
            {
                System.out.println(i + " " + cacheKey + " " + cacheIdx);
                break;
            }
        }
        indexWriter.flush();

    }
}
