package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestFileMetadata
{
    @Test
    public void test()
    {
        PixelsReader pixelsReader = null;
        //String filePath = "hdfs://presto00:9000/pixels/testNull_pixels/201806190954180.pxl";
        String filePath = "hdfs://presto00:9000/pixels/pixels/testnull_pixels/v_0_order/";
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try
        {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            FileStatus[] fileStatuses = fs.listStatus(path);
            int i = 0;
            for (FileStatus fileStatus : fileStatuses)
            {
                pixelsReader = PixelsReaderImpl.newBuilder()
                        .setFS(fs)
                        .setPath(fileStatus.getPath())
                        .build();
//                System.out.println(pixelsReader.getRowGroupNum());
                if (pixelsReader.getFooter().getRowGroupStatsList().size() != 1)
                {
                    System.out.println("Path: " + fileStatus.getPath() + ", RGNum: " + pixelsReader.getRowGroupNum());
                }
                i++;
                pixelsReader.close();
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
