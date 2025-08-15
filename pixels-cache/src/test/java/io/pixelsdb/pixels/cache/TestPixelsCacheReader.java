/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author guodong
 */
public class TestPixelsCacheReader
{

    @Test
    public void testReader()
    {
        try
        {
            MemoryMappedFile zoneCacheFile1 = new MemoryMappedFile("/Users/Jelly/Desktop/pixels.cache/zone1", 64000000L);
            MemoryMappedFile zoneCacheFile2 = new MemoryMappedFile("/Users/Jelly/Desktop/pixels.cache/zone2", 64000000L);
            MemoryMappedFile globalIndexFile = new MemoryMappedFile("/Users/Jelly/Desktop/pixels.index/global", 64000000L);
            MemoryMappedFile zoneIndexFile1 = new MemoryMappedFile("/Users/Jelly/Desktop/pixels.index/zone1", 64000000L);
            MemoryMappedFile zoneIndexFile2 = new MemoryMappedFile("/Users/Jelly/Desktop/pixels.index/zone2", 64000000L);
            PixelsCacheReader cacheReader = PixelsCacheReader.newBuilder()
                    .setIndexFiles(Arrays.asList(zoneIndexFile1, zoneIndexFile2), globalIndexFile)
                    .setCacheFiles(Arrays.asList(zoneCacheFile1, zoneCacheFile2), 1)
                    .build();
            int index = 0;
            for (short i = 0; i < 1000; i++)
            {
                for (short j = 0; j < 64; j++)
                {
                    ByteBuffer value = cacheReader.get(-1, i, j, false);
                    if (value != null)
                    {
                        assert value.getInt() == index;
                        index++;
                    }
                }
            }
            cacheReader.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
