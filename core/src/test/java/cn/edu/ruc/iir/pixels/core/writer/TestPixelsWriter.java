package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.TestParams;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
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
        String filePath = TestParams.filePath;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            LongColumnVector x = (LongColumnVector) rowBatch.cols[0];
            BytesColumnVector y = (BytesColumnVector) rowBatch.cols[1];

            PixelsWriter pixelsWriter =
                    PixelsWriter.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(10000)
                            .setRowGroupSize(64*1024*1024)
                            .setFS(fs)
                            .setFilePath(new Path(filePath))
                            .setBlockSize(1024*1024*1024)
                            .setReplication((short) 1)
                            .setBlockPadding(false)
                            .setEncoding(true)
                            .build();

            for (int i = 0; i < TestParams.rowNum; i++)
            {
                int row = rowBatch.size++;
                x.vector[row] = i;
                y.setVal(row, String.valueOf(i).getBytes());
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
