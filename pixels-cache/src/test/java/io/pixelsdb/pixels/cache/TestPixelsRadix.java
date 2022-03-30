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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author guodong
 */
public class TestPixelsRadix
{
    private final int TEST_NUM = 1_000_000;

    @Test
    public void testBasic()
    {
        PixelsRadix radix = new PixelsRadix();
        PixelsCacheIdx cacheIdx0 = new PixelsCacheIdx(0, 0);
        PixelsCacheKey cacheKey0 = new PixelsCacheKey(0, (short) 0, (short) 0);
        radix.put(cacheKey0, cacheIdx0);
        // MATCH_END_AT_END_EDGE
        PixelsCacheIdx cacheIdx1 = new PixelsCacheIdx(1, 1);
        PixelsCacheKey cacheKey1 = new PixelsCacheKey(1, (short) 0, (short) 0);
        radix.put(cacheKey1, cacheIdx1);
        // EXACT_MATCH
        PixelsCacheIdx cacheIdx2 = new PixelsCacheIdx(2, 2);
        PixelsCacheKey cacheKey2 = new PixelsCacheKey(1, (short) 0, (short) 0);
        radix.put(cacheKey2, cacheIdx2);
        // MATCH_END_AT_MID_EDGE
        PixelsCacheIdx cacheIdx3 = new PixelsCacheIdx(3, 3);
        PixelsCacheKey cacheKey3 = new PixelsCacheKey(1, (short) 0, (short) 1);
        radix.put(cacheKey3, cacheIdx3);

        assertEquals(cacheIdx0, radix.get(0, (short) 0, (short) 0));
        assertEquals(cacheIdx2, radix.get(1, (short) 0, (short) 0));
        assertEquals(cacheIdx3, radix.get(1, (short) 0, (short) 1));

        radix.remove(0, (short) 0, (short) 0);
        radix.remove(1, (short) 0, (short) 1);
        radix.remove(1, (short) 0, (short) 0);
    }

    @Test
    public void testLargeNum()
    {
        PixelsRadix radix = new PixelsRadix();
        for (int i = 0; i < TEST_NUM; i++)
        {
            PixelsCacheIdx cacheIdx = new PixelsCacheIdx(i, i);
            PixelsCacheKey cacheKey = new PixelsCacheKey(i, (short) i, (short) i);
            radix.put(cacheKey, cacheIdx);
        }
        for (int i = 0; i < TEST_NUM; i++)
        {
            PixelsCacheIdx expectedCacheIdx = new PixelsCacheIdx(i, i);
            PixelsCacheIdx cacheIdx = radix.get(i, (short) i, (short) i);
            assertEquals(expectedCacheIdx, cacheIdx);
        }
    }
}
