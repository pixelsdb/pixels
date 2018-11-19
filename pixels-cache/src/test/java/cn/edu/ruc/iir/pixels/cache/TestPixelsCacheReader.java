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
                    .setCacheLocation("/Users/Jelly/Desktop/pixels.cache")
                    .setCacheSize(1024*1024*64L)
                    .setIndexLocation("/Users/Jelly/Desktop/pixels.index")
                    .setIndexSize(1024*1024*64L)
                    .build();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
