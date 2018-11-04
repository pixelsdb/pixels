package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsCacheReader
{
    @Test
    public void testReader()
    {
        try {
            PixelsCacheReader cacheReader = PixelsCacheReader.newBuilder()
                    .setCacheLocation("")
                    .setCacheSize(11L)
                    .setIndexLocation("")
                    .setIndexSize(11L)
                    .build();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
