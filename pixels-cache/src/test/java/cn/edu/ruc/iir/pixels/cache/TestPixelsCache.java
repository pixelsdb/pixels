package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsCache
{
    @Test
    public void testStartup()
    {
        try {
            PixelsCache pixelsCache = PixelsCache.newBuilder()
                    .setCacheLocation("/Users/Jelly/Desktop/cachefile")
                    .setCacheSize(1024*1024*100L)
                    .setIndexLocation("/Users/Jelly/Desktop/indexfile")
                    .setIndexSize(1024*1024*100L)
                    .setScheduleSeconds(300)
                    .setMQLocation("/Users/Jelly/Desktop/mqfile")
                    .setMQSize(1024*1024*10L)
                    .setMQRecordSize(128)
                    .build();
            pixelsCache.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
