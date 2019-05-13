package cn.edu.ruc.iir.pixels.cache;

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
        PixelsCacheKey cacheKey0 = new PixelsCacheKey(0, (short) 0, (short) 0);
        PixelsCacheIdx cacheIdx0 = new PixelsCacheIdx(0, 0);
        radix.put(cacheKey0, cacheIdx0);
        // MATCH_END_AT_END_EDGE
        PixelsCacheKey cacheKey1 = new PixelsCacheKey(1, (short) 0, (short) 0);
        PixelsCacheIdx cacheIdx1 = new PixelsCacheIdx(1, 1);
        radix.put(cacheKey1, cacheIdx1);
        // EXACT_MATCH
        PixelsCacheKey cacheKey2 = new PixelsCacheKey(1, (short) 0, (short) 0);
        PixelsCacheIdx cacheIdx2 = new PixelsCacheIdx(2, 2);
        radix.put(cacheKey2, cacheIdx2);
        // MATCH_END_AT_MID_EDGE
        PixelsCacheKey cacheKey3 = new PixelsCacheKey(1, (short) 0, (short) 1);
        PixelsCacheIdx cacheIdx3 = new PixelsCacheIdx(3, 3);
        radix.put(cacheKey3, cacheIdx3);

        assertEquals(cacheIdx0, radix.get(cacheKey0));
        assertEquals(cacheIdx2, radix.get(cacheKey2));
        assertEquals(cacheIdx3, radix.get(cacheKey3));

        radix.remove(cacheKey0);
        radix.remove(cacheKey3);
        radix.remove(cacheKey2);
    }

    @Test
    public void testLargeNum()
    {
        PixelsRadix radix = new PixelsRadix();
        for (int i = 0; i < TEST_NUM; i++)
        {
            PixelsCacheKey cacheKey = new PixelsCacheKey(i, (short) i, (short) i);
            PixelsCacheIdx cacheIdx = new PixelsCacheIdx(i, i);
            radix.put(cacheKey, cacheIdx);
        }
        for (int i = 0; i < TEST_NUM; i++)
        {
            PixelsCacheKey cacheKey = new PixelsCacheKey(i, (short) i, (short) i);
            PixelsCacheIdx expectedCacheIdx = new PixelsCacheIdx(i, i);
            PixelsCacheIdx cacheIdx = radix.get(cacheKey);
            assertEquals(expectedCacheIdx, cacheIdx);
        }
    }
}
