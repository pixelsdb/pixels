package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsWriter
{
    @Test
    public void test()
    {
        final int ROWNUM = 100000000;

        String fileP = "hdfs://127.0.0.1:9000/test1.pxl";
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        String schemaStr = "struct<x:int,y:int>";
        try {
            FileSystem fs = FileSystem.get(URI.create(fileP), conf);
            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            LongColumnVector x = (LongColumnVector) rowBatch.cols[0];
            LongColumnVector y = (LongColumnVector) rowBatch.cols[1];

            PixelsWriter pixelsWriter =
                    PixelsWriter.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(10000)
                            .setRowGroupSize(64)
                            .setFS(fs)
                            .setFilePath(new Path(fileP))
                            .setBlockSize(1048576000)
                            .setReplication((short) 1)
                            .setBlockPadding(false)
                            .build();

            for (int i = 0; i < ROWNUM; i++)
            {
                int row = rowBatch.size++;
                x.vector[row] = i;
                y.vector[row] = i * 2;
                if (rowBatch.size == rowBatch.getMaxSize()) {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            }
            if (rowBatch.size != 0) {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
            }
            pixelsWriter.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
