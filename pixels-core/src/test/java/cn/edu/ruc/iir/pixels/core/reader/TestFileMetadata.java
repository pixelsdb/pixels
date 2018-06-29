package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class TestFileMetadata
{
    @Test
    public void test ()
    {
        PixelsReader pixelsReader = null;
        //String filePath = "hdfs://presto00:9000/pixels/testNull_pixels/201806190954180.pxl";
        String filePath = "hdfs://presto00:9000/pixels/pixels/testnull_pixels/v_0_compact/compact.pxl";
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(fs)
                    .setPath(path)
                    .build();
            List<String> fieldNames = pixelsReader.getFileSchema().getFieldNames();
            for (String name : fieldNames)
            {
                System.out.println(name);
            }
            System.out.println(pixelsReader.getRowGroupNum());
            System.out.println(fieldNames.size());
            System.out.println(pixelsReader.getRowGroupFooter(0).getRowGroupIndexEntry().getColumnChunkIndexEntries(0).getChunkLength());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
