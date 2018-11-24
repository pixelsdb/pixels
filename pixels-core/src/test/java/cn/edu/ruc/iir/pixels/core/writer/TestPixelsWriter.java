package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.cache.PixelsCacheConfig;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheKey;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheReader;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheWriter;
import cn.edu.ruc.iir.pixels.cache.PixelsPhysicalReader;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TestParams;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.exception.PixelsWriterException;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsWriter {

    @Test
    public void testWriterWithNull() {
        String filePath = TestParams.filePath;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        // schema: struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);
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
            for (int i = 0; i < TestParams.rowNum; i++) {
                int row = rowBatch.size++;
                if (i % 100 == 0)
                {
                    a.isNull[row] = true;
                    a.vector[row] = 0;
                    b.isNull[row] = true;
                    b.vector[row] = 0;
                    c.isNull[row] = true;
                    c.vector[row] = 0;
                    d.isNull[row] = true;
                    d.time[row] = 0;
                    d.nanos[row] = 0;
                    e.isNull[row] = true;
                    e.vector[row] = 0;
                    z.isNull[row] = true;
                    z.vector[row] = new byte[0];
                }
                else
                {
                    a.vector[row] = i;
                    a.isNull[row] = false;
                    b.vector[row] = i * 3.1415f;
                    b.isNull[row] = false;
                    c.vector[row] = i * 3.14159d;
                    c.isNull[row] = false;
                    d.set(row, timestamp);
                    d.isNull[row] = false;
                    e.vector[row] = i > 25 ? 1 : 0;
                    e.isNull[row] = false;
                    z.setVal(row, String.valueOf(i).getBytes());
                    z.isNull[row] = false;
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

    @Test
    public void testWriterWithoutNull()
    {
        String filePath = TestParams.filePath;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        // schema: struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);
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
            for (int i = 0; i < TestParams.rowNum; i++) {
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

    @Test
    public void testRead()
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"a", "b", "c", "d", "e", "z"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        VectorizedRowBatch rowBatch;
        PixelsReader pixelsReader;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(TestParams.filePath), conf);
            Path path = new Path(TestParams.filePath);
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(fs)
                    .setPath(path)
                    .build();
            PixelsRecordReader recordReader = pixelsReader.read(option);
            rowBatch = recordReader.readBatch(5000);
            LongColumnVector acv = (LongColumnVector) rowBatch.cols[0];
            DoubleColumnVector bcv = (DoubleColumnVector) rowBatch.cols[1];
            DoubleColumnVector ccv = (DoubleColumnVector) rowBatch.cols[2];
            TimestampColumnVector dcv = (TimestampColumnVector) rowBatch.cols[3];
            LongColumnVector ecv = (LongColumnVector) rowBatch.cols[4];
            BytesColumnVector zcv = (BytesColumnVector) rowBatch.cols[5];
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void prepareCacheData()
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
                                     .setCacheSize(1024*1024*128L)
                                     .setIndexLocation("/Users/Jelly/Desktop/pixels.index")
                                     .setIndexSize(1024*1024*128L)
                                     .setOverwrite(true)
                                     .setFS(fs)
                                     .build();
            String directory = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_2_compact";
            int[] rgs = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
            int[] cols = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
            long cacheLength = 0L;
            FileStatus[] fileStatuses = fs.listStatus(new Path(directory));
            long startNano = System.nanoTime();
            // write cache
            for (FileStatus fileStatus : fileStatuses)
            {
                Path file = fileStatus.getPath();
//                PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(fs, file);
                for (int i = 0; i < 16; i++)
                {
                    for (int j = 0; j < 105; j++) {
//                        PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(i);
//                        PixelsProto.ColumnChunkIndex chunkIndex =
//                                rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(j);
//                        int chunkLen = (int) chunkIndex.getChunkLength();
//                        long chunkOffset = chunkIndex.getChunkOffset();
                        cacheLength += 118000;
//                        byte[] columnlet = pixelsPhysicalReader.read(chunkOffset, chunkLen);
                        byte[] columnlet = new byte[0];
                        PixelsCacheKey cacheKey = new PixelsCacheKey(file.getName(), (short) i, (short) j);
                        cacheWriter.write(cacheKey, columnlet);
                    }
                }
            }
            long endNano = System.nanoTime();
            System.out.println("Time cost: " + (endNano - startNano) + "ns");
            System.out.println("Total length: " + cacheLength);
            long flushStartNano = System.nanoTime();
            // flush index
            cacheWriter.flush();
            long flushEndNano = System.nanoTime();
            System.out.println("Flush time cost: " + (flushEndNano - flushStartNano) + "ns");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void validateCache()
            throws Exception
    {
        // get fs
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        FileSystem fs = FileSystem.get(URI.create(cacheConfig.getWarehousePath()), conf);
        int[] rgs = new int[]{0, 1, 2, 3, 4, 5};
        int[] cols = new int[]{8, 8, 0, 2, 4, 9};
        // validation
        PixelsCacheReader cacheReader = PixelsCacheReader.newBuilder()
                                                         .setCacheLocation("/Users/Jelly/Desktop/pixels.cache")
                                                         .setCacheSize(1024*1024*128L)
                                                         .setIndexLocation("/Users/Jelly/Desktop/pixels.index")
                                                         .setIndexSize(1024*1024*128L)
                                                         .build();
        Path file = new Path("hdfs://dbiir01:9000/pixels/pixels/test_105/v_2_compact/62_2018092323481262.compact.pxl");
//        for (FileStatus fileStatus : fs.listStatus(new Path(directory)))
//        {
//            Path file = fileStatus.getPath();
            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(fs, file);
            for (short i = 0; i < rgs.length; i++)
            {
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rgs[i]);
                PixelsProto.ColumnChunkIndex chunkIndex =
                        rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(cols[i]);
                int chunkLen = (int) chunkIndex.getChunkLength();
                long chunkOffset = chunkIndex.getChunkOffset();
                byte[] expectedColumnlet = pixelsPhysicalReader.read(chunkOffset, chunkLen);
                byte[] actualColumnlet = cacheReader.get(file.getName(), (short) rgs[i], (short) cols[i]);
                for (int j = 0; j < expectedColumnlet.length; j++)
                {
                    byte exp = expectedColumnlet[j];
                    byte act = actualColumnlet[j];
                    if (exp != act) {
                        System.out.println(j + ", expected: " + exp + ", actual: " + act);
                    }
                }
                if (!Arrays.equals(expectedColumnlet, actualColumnlet)) {
                    System.out.println(file.getName() + "-" + rgs[i] + "-" + cols[i]);
                }
            }
//        }
    }
}
