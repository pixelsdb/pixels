package cn.edu.ruc.iir.pixels.load.rw;

import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.core.*;
import cn.edu.ruc.iir.pixels.core.exception.PixelsWriterException;
import cn.edu.ruc.iir.pixels.core.vector.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.load.rw
 * @ClassName: PixelsCoreTest
 * @Description:
 * @author: tao
 * @date: Create in 2018-11-07 16:05
 **/
public class PixelsCoreTest {

    String hdfsDir = "/home/tao/data/hadoop-2.7.3/etc/hadoop/"; // dbiir10

    @Test
    public void testReadPixelsFile() throws FSException {
        String pixelsFile = "hdfs://dbiir10:9000/pixels/pixels/test_105/20181117213047_3239.pxl";
        FSFactory fsFactory = FSFactory.Instance(hdfsDir);
        Path file = new Path(pixelsFile);
        FileSystem fs = fsFactory.getFileSystem().get();

        PixelsReader pixelsReader = null;
        try {
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(fs)
                    .setPath(file)
                    .build();
            System.out.println(pixelsReader.getRowGroupNum());
            System.out.println(pixelsReader.getRowGroupInfo(0).toString());
            System.out.println(pixelsReader.getRowGroupInfo(1).toString());
            if (pixelsReader.getFooter().getRowGroupStatsList().size() != 1) {
                System.out.println("Path: " + file + ", RGNum: " + pixelsReader.getRowGroupNum());
            }

            pixelsReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWritePixelsFile() throws FSException {
        String pixelsFile = "hdfs://dbiir10:9000//pixels/pixels/test_105/v_0_order/.pxl";
        FSFactory fsFactory = FSFactory.Instance(hdfsDir);
        Path file = new Path(pixelsFile);
        FileSystem fs = fsFactory.getFileSystem().get();

        // schema: struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>
        String schemaStr = "struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>";

        try {
            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            LongColumnVector a = (LongColumnVector) rowBatch.cols[0];              // int
            DoubleColumnVector b = (DoubleColumnVector) rowBatch.cols[1];          // float
            DoubleColumnVector c = (DoubleColumnVector) rowBatch.cols[2];          // double
            TimestampColumnVector d = (TimestampColumnVector) rowBatch.cols[3];    // timestamp
            LongColumnVector e = (LongColumnVector) rowBatch.cols[4];              // boolean
            BytesColumnVector z = (BytesColumnVector) rowBatch.cols[5];            // string

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(10000)
                            .setRowGroupSize(64 * 1024 * 1024)
                            .setFS(fs)
                            .setFilePath(file)
                            .setBlockSize(256 * 1024 * 1024)
                            .setReplication((short) 3)
                            .setBlockPadding(true)
                            .setEncoding(true)
                            .setCompressionBlockSize(1)
                            .build();

            long curT = System.currentTimeMillis();
            Timestamp timestamp = new Timestamp(curT);
            for (int i = 0; i < 1; i++) {
                int row = rowBatch.size++;
                a.vector[row] = i;
                a.isNull[row] = false;
                b.vector[row] = i * 3.1415f;
                b.isNull[row] = false;
                c.vector[row] = i * 3.14159d;
                c.isNull[row] = false;
                d.set(row, timestamp);
                d.isNull[row] = false;
                e.vector[row] = i > 25000 ? 1 : 0;
                e.isNull[row] = false;
                z.setVal(row, String.valueOf(i).getBytes());
                z.isNull[row] = false;
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
        } catch (IOException | PixelsWriterException e) {
            e.printStackTrace();
        }
    }

}
