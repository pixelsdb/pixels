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
//        try {
//            PixelsCacheReader cacheReader = PixelsCacheReader.newBuilder()
//                    .setCacheLocation("/Users/Jelly/Desktop/pixels.cache")
//                    .setCacheSize(1024*1024*64L)
//                    .setIndexLocation("/Users/Jelly/Desktop/pixels.index")
//                    .setIndexSize(1024*1024*64L)
//                    .build();
//            String path = "/pixels/pixels/test_105/2121211211212.pxl";
//            int index = 0;
//            for (short i = 0; i < 1000; i++) {
//                for (short j = 0; j < 64; j++) {
//                    byte[] value = cacheReader.get(path, i, j);
//                    ByteBuffer buffer = ByteBuffer.wrap(value);
//                    assert buffer.getInt() == index;
//                    index++;
//                }
//            }
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
