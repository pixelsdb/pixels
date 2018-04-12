package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TestParams;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.exception.PixelsWriterException;
import cn.edu.ruc.iir.pixels.core.vector.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.regex.Pattern;

public class TestWriter {

    @Test
    public void testWriter() {

        String filePath = "hdfs://presto00:9000/po_compare/point_20.pxl";
        String tSchema = "struct<orderkey:int,orderstatus:double,totalprice:double>";
        int rowNum = 20;

        int start = tSchema.indexOf("<");
        int end = tSchema.indexOf(">");
        String content = tSchema.substring(start + 1, end);
        System.out.println(content);

        Pattern pattern = Pattern.compile(",");
        String[] columns = pattern.split(content.toString(), 0);
        pattern = Pattern.compile(":");

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            TypeDescription schema = TypeDescription.fromString(tSchema);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            int len = columns.length;
            ColumnVector[] columnVectors = new ColumnVector[len];
            for (int i = 0; i < len; i++) {
                String column = columns[i];
                String[] c = pattern.split(column, 0);
                System.out.println(Arrays.asList(c).toString());
                if (c[1].equals("int")) {
                    LongColumnVector a = (LongColumnVector) rowBatch.cols[i];
                    columnVectors[i] = a;
                } else if (c[1].equals("double")) {
                    DoubleColumnVector b = (DoubleColumnVector) rowBatch.cols[i];
                    columnVectors[i] = b;
                } else if (c[1].equals("string")) {
                    BytesColumnVector z = (BytesColumnVector) rowBatch.cols[i];
                    columnVectors[i] = z;
                }
            }

            System.out.println(columnVectors.length);

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(TestParams.pixelStride)
                            .setRowGroupSize(TestParams.rowGroupSize)
                            .setFS(fs)
                            .setFilePath(new Path(filePath))
                            .setBlockSize(TestParams.blockSize)
                            .setReplication(TestParams.blockReplication)
                            .setBlockPadding(TestParams.blockPadding)
                            .setEncoding(TestParams.encoding)
                            .setCompressionBlockSize(TestParams.compressionBlockSize)
                            .build();

            long curT = System.currentTimeMillis();
            Timestamp timestamp = new Timestamp(curT);
            System.out.println(curT + ", nanos: " + timestamp.getNanos() + ",  time: " + timestamp.getTime());
            for (int i = 0; i < rowNum; i++) {
                int row = rowBatch.size++;
                for (int j = 0; j < len; j++) {
                    String column = columns[j];
                    String[] c = pattern.split(column, 0);
                    if (c[1].equals("int")) {
                        columnVectors[j].add("" + i);
                    } else if (c[1].equals("double")) {
                        columnVectors[j].add("" + i * 1);
                    } else if (c[1].equals("string")) {
                        columnVectors[j].add("" + i * 2);
                    }
                }

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
