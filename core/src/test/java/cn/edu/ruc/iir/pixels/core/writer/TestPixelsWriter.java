package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
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
        final int ROWNUM = 10000;

        String filePath = TestParams.filePath;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        String schemaStr = "struct<x:double,y:string>";
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            DoubleColumnVector x = (DoubleColumnVector) rowBatch.cols[0];
            BytesColumnVector y = (BytesColumnVector) rowBatch.cols[1];

            PixelsWriter pixelsWriter =
                    PixelsWriter.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(1000)
                            .setRowGroupSize(64*1024*1024)
                            .setFS(fs)
                            .setFilePath(new Path(filePath))
                            .setBlockSize(1024*1024*1024)
                            .setReplication((short) 1)
                            .setBlockPadding(false)
                            .build();

            for (int i = 0; i < ROWNUM; i++)
            {
                int row = rowBatch.size++;
                x.vector[row] = i * 1.2;
                y.vector[row] = "test".getBytes();
                if (rowBatch.size == rowBatch.getMaxSize()) {
                    //long start = System.currentTimeMillis();
                    pixelsWriter.addRowBatch(rowBatch);
                    //System.out.println("add rb:" + (System.currentTimeMillis()-start));
                    //start = System.currentTimeMillis();
                    rowBatch.reset();
                    //System.out.println("reset: " + (System.currentTimeMillis()-start));
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
