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
package io.pixelsdb.pixels.cache;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TestCacheContentReader {

    // TODO: scan all items, do a sanity check
    // the cache content can be generated by FakeCacheGenerator
    String cacheContentFile = "/mnt/nvme1n1/pixels.cache";

    @Before
    public void prepare ()
    {
        cacheContentFile = "/mnt/nvme1n1/pixels.cache";
    }

    @Test
    public void testDiskCacheContentReader() throws IOException, NoSuchFieldException, IllegalAccessException {
        CacheContentReader reader = new DiskCacheContentReader(cacheContentFile);

        // 40125714472, 4608
        // 4633903504, 4608
        PixelsCacheIdx idx = new PixelsCacheIdx(40125714472L, 4608);
        ByteBuffer buf = ByteBuffer.allocate(idx.length);
        buf.limit(idx.length);
        reader.read(idx, buf);
        System.out.println(StandardCharsets.UTF_8.decode(buf));
        buf.position(0);
        reader.read(new PixelsCacheIdx(4633903504L, 4608), buf);
        System.out.println(StandardCharsets.UTF_8.decode(buf));
    }
}
