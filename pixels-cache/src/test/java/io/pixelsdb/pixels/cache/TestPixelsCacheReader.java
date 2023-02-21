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

import java.nio.ByteBuffer;

/**
 * @author guodong
 */
public class TestPixelsCacheReader
{

    @Test
    public void testSimpleSearchAndGet() throws Exception {
        MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.index", 102400000);

        PixelsCacheKey key = new PixelsCacheKey(1073747711, (short) 0, (short) 191);
        PixelsCacheReader cacheReader = PixelsCacheReader
                .newBuilder()
                .setIndexFile(indexFile)
                .build();
        System.out.println(cacheReader.search(key.blockId, key.rowGroupId, key.columnId));
    }

    @Test
    public void testReader()
    {
        try
        {
            MemoryMappedFile cacheFile = new MemoryMappedFile("/Users/Jelly/Desktop/pixels.cache", 64000000L);
            MemoryMappedFile indexFile = new MemoryMappedFile("/Users/Jelly/Desktop/pixels.index", 64000000L);
            PixelsCacheReader cacheReader = PixelsCacheReader
                    .newBuilder()
                    .setIndexFile(indexFile)
                    .setCacheFile(cacheFile)
                    .build();
            String path = "/pixels/pixels/test_105/2121211211212.pxl";
            int index = 0;
            for (short i = 0; i < 1000; i++)
            {
                for (short j = 0; j < 64; j++)
                {
                    ByteBuffer value = cacheReader.get(-1, i, j);
                    if (value != null)
                    {
                        assert value.getInt() == index;
                        index++;
                    }
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
