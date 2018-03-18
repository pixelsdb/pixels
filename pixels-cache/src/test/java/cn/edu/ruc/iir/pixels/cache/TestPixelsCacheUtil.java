package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsCacheUtil
{
    @Test
    public void testHeader()
    {
        int header = 0;
        // start writing
        header = PixelsCacheUtil.setHeaderRW(header, true);
        System.out.println(PixelsCacheUtil.getHeaderRW(header));

        // done writing
        header = PixelsCacheUtil.setHeaderRW(header, false);
        System.out.println(PixelsCacheUtil.getHeaderRW(header));

        // start reading
        header = PixelsCacheUtil.incrementReadCount(header);
        header = PixelsCacheUtil.incrementReadCount(header);
        System.out.println(PixelsCacheUtil.getHeaderRW(header));
        System.out.println(PixelsCacheUtil.getReadCount(header));

        // done reading
        header = PixelsCacheUtil.decrementReadCount(header);
        header = PixelsCacheUtil.decrementReadCount(header);

        // start writing
        header = PixelsCacheUtil.setHeaderRW(header, true);
        System.out.println(PixelsCacheUtil.getHeaderRW(header));
        System.out.println(PixelsCacheUtil.getReadCount(header));
    }

    @Test
    public void test()
    {
        int header = 0;
        int size = 536_870_911;
        System.out.println(size);
        header = header | (1 << 31);
        header = header | (1 << 30);
        header = header | (1 << 29);
        header = header | size;
        System.out.println(header >> 31 & 1);
        System.out.println(header >> 30 & 1);
        System.out.println(header >> 29 & 1);
        System.out.println(header & 0x1fffffff);
    }
}
