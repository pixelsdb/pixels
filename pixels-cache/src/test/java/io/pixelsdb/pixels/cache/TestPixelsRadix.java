package io.pixelsdb.pixels.cache;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * pixels
 *
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
        radix.put(0, (short) 0, (short) 0, cacheIdx0);
        // MATCH_END_AT_END_EDGE
        PixelsCacheIdx cacheIdx1 = new PixelsCacheIdx(1, 1);
        radix.put(1, (short) 0, (short) 0, cacheIdx1);
        // EXACT_MATCH
        PixelsCacheIdx cacheIdx2 = new PixelsCacheIdx(2, 2);
        radix.put(1, (short) 0, (short) 0, cacheIdx2);
        // MATCH_END_AT_MID_EDGE
        PixelsCacheIdx cacheIdx3 = new PixelsCacheIdx(3, 3);
        radix.put(1, (short) 0, (short) 1, cacheIdx3);

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
            radix.put(i, (short) i, (short) i, cacheIdx);
        }
        for (int i = 0; i < TEST_NUM; i++)
        {
            PixelsCacheIdx expectedCacheIdx = new PixelsCacheIdx(i, i);
            PixelsCacheIdx cacheIdx = radix.get(i, (short) i, (short) i);
            assertEquals(expectedCacheIdx, cacheIdx);
        }
    }
}
