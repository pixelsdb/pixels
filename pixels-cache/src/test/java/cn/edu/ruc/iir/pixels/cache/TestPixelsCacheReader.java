package cn.edu.ruc.iir.pixels.cache;

import org.junit.Test;

import java.nio.ByteBuffer;

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
        try
        {
            MemoryMappedFile cacheFile = new MemoryMappedFile("/Users/Jelly/Desktop/pixels.cache", 64000000L);
            MemoryMappedFile indexFile = new MemoryMappedFile("/Users/Jelly/Desktop/pixels.index", 64000000L);
            PixelsCacheReader cacheReader = PixelsCacheReader
                    .newBuilder()
                    .setIndexFile(indexFile)
                    .setCacheFile(cacheFile)
                    .build();
            String path = "/pixels/pixels/test_105/2121211211212.pxl";
            int index = 0;
            for (short i = 0; i < 1000; i++)
            {
                for (short j = 0; j < 64; j++)
                {
                    byte[] value = cacheReader.get(-1, i, j);
                    ByteBuffer buffer = ByteBuffer.wrap(value);
                    assert buffer.getInt() == index;
                    index++;
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
