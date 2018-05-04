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
                    .setMQLocation("")
                    .setMQFileSize(11L)
                    .setMQRecordSize(1)
                    .setMQAppend(true)
                    .build();

            byte[] content = cacheReader.get(1, (short) 1, (short) 1);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
