package cn.edu.ruc.iir.pixels.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.net.URI;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsCacheWriter
{
    @Test
    public void testCacheWriter()
    {
        try {
            // get fs
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
            FileSystem fs = FileSystem.get(URI.create(cacheConfig.getWarehousePath()), conf);
            PixelsCacheWriter cacheWriter =
                    PixelsCacheWriter.newBuilder()
                                     .setCacheLocation("/Users/Jelly/Desktop/pixels.cache")
                                     .setCacheSize(1024*1024*64L)
                                     .setIndexLocation("/Users/Jelly/Desktop/pixels.index")
                                     .setIndexSize(1024*1024*64L)
                                     .setOverwrite(true)
                                     .setFS(fs)
                                     .build();
            String path = "/pixels/pixels/test_105/2121211211212.pxl";
            int index = 0;
            for (short i = 0; i < 1000; i++)
            {
                for (short j = 0; j < 64; j++) {
                    PixelsCacheKey cacheKey = new PixelsCacheKey(path, i, j);
                    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(index++);
                    byte[] value = byteBuffer.array();
                    cacheWriter.write(cacheKey, value);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
