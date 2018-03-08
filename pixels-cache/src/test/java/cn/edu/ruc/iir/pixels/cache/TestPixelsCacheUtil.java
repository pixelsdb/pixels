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
}
