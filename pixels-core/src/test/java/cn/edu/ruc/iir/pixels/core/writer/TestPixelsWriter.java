package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.cache.PixelsCacheConfig;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheKey;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheWriter;
import cn.edu.ruc.iir.pixels.cache.PixelsPhysicalReader;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
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
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsWriter
{

    @Test
    public void testWriterWithNull()
    {
        String filePath = TestParams.filePath;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        // schema: struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>
        try
        {
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
            for (int i = 0; i < TestParams.rowNum; i++)
            {
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
                if (rowBatch.size == rowBatch.getMaxSize())
                {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            }
            if (rowBatch.size != 0)
            {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
            }
            pixelsWriter.close();
        }
        catch (IOException | PixelsWriterException e)
        {
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
        try
        {
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
            for (int i = 0; i < TestParams.rowNum; i++)
            {
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
                if (rowBatch.size == rowBatch.getMaxSize())
                {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            }
            if (rowBatch.size != 0)
            {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
            }
            pixelsWriter.close();
        }
        catch (IOException | PixelsWriterException e)
        {
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

        try
        {
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
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void prepareCacheData()
    {
        try
        {
            // get fs
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
            FileSystem fs = FileSystem.get(URI.create(cacheConfig.getWarehousePath()), conf);
            PixelsCacheWriter cacheWriter =
                    PixelsCacheWriter.newBuilder()
                                     .setCacheLocation("/Users/Jelly/Desktop/pixels.cache")
                                     .setCacheSize(1024 * 1024 * 1024L)
                                     .setIndexLocation("/Users/Jelly/Desktop/pixels.index")
                                     .setIndexSize(1024 * 1024 * 1024L)
                                     .setOverwrite(true)
                                     .setFS(fs)
                                     .build();
            String directory = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_1_compact";
            long cacheLength = 0L;
            FileStatus[] fileStatuses = fs.listStatus(new Path(directory));
            MetadataService metadataService = new MetadataService("dbiir10", 18888);
            Layout layout = metadataService.getLayout("pixels", "test_105", 0).get(0);
            Compact compact = layout.getCompactObject();
            int cacheBorder = compact.getCacheBorder();
            List<String> cacheOrders = compact.getColumnletOrder().subList(0, cacheBorder);
            long startNano = System.nanoTime();
            // write cache
            for (FileStatus fileStatus : fileStatuses)
            {
                Path file = fileStatus.getPath();
                PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(fs, file);
                for (int i = 0; i < cacheBorder; i++)
                {
                    String[] cacheColumnletIdParts = cacheOrders.get(i).split(":");
                    short cacheRGId = Short.parseShort(cacheColumnletIdParts[0]);
                    short cacheColId = Short.parseShort(cacheColumnletIdParts[1]);
                    PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(cacheRGId);
                    PixelsProto.ColumnChunkIndex chunkIndex =
                            rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(cacheColId);
                    int chunkLen = (int) chunkIndex.getChunkLength();
                    long chunkOffset = chunkIndex.getChunkOffset();
                    cacheLength += chunkLen;
                    byte[] columnlet = pixelsPhysicalReader.read(chunkOffset, chunkLen);
//                  byte[] columnlet = new byte[0];
                    PixelsCacheKey cacheKey = new PixelsCacheKey(file.toString(), cacheRGId, cacheColId);
                    cacheWriter.write(cacheKey, columnlet);
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
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void validateCache()
            throws Exception
    {
        // get fs
//        Configuration conf = new Configuration();
//        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
//        FileSystem fs = FileSystem.get(URI.create(cacheConfig.getWarehousePath()), conf);
//        // validation
//        PixelsCacheReader cacheReader = PixelsCacheReader.newBuilder()
//                                                         .setCacheLocation("/Users/Jelly/Desktop/pixels.cache")
//                                                         .setCacheSize(1024*1024*128L)
//                                                         .setIndexLocation("/Users/Jelly/Desktop/pixels.index")
//                                                         .setIndexSize(1024*1024*128L)
//                                                         .build();
//        String directory = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_2_compact";
//        for (FileStatus fileStatus : fs.listStatus(new Path(directory)))
//        {
//            Path file = fileStatus.getPath();
//            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(fs, file);
//            for (short i = 0; i < 16; i++)
//            {
//                PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(i);
//                for (short j = 0; j < 105; j++)
//                {
//                    PixelsProto.ColumnChunkIndex chunkIndex =
//                            rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(j);
//                    int chunkLen = (int) chunkIndex.getChunkLength();
//                    long chunkOffset = chunkIndex.getChunkOffset();
//                    byte[] expectedColumnlet = pixelsPhysicalReader.read(chunkOffset, chunkLen);
//                    byte[] actualColumnlet = cacheReader.get(file.toString(), i, j);
//                    for (int k = 0; k < expectedColumnlet.length; k++)
//                    {
//                        byte exp = expectedColumnlet[k];
//                        byte act = actualColumnlet[k];
//                        if (exp != act) {
//                            System.out.println(k + ", expected: " + exp + ", actual: " + act);
//                        }
//                    }
//                    if (!Arrays.equals(expectedColumnlet, actualColumnlet)) {
//                        System.out.println(file.getName() + "-" + i + "-" + j);
//                    }
//                }
//            }
//        }
    }

    @Test
    public void validateReader()
            throws Exception
    {
        // get fs
//        Configuration conf = new Configuration();
//        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
//        FileSystem fs = FileSystem.get(URI.create(cacheConfig.getWarehousePath()), conf);
//        // validation
//        PixelsCacheReader cacheReader = PixelsCacheReader.newBuilder()
//                                                         .setCacheLocation("/Users/Jelly/Desktop/pixels.cache")
//                                                         .setCacheSize(1024*1024*1024L)
//                                                         .setIndexLocation("/Users/Jelly/Desktop/pixels.index")
//                                                         .setIndexSize(1024*1024*1024L)
//                                                         .build();
//        MetadataService metadataService = new MetadataService("dbiir10", 18888);
//        Layout layout = metadataService.getLayout("pixels", "test_105", 0).get(0);
//        Compact compact = layout.getCompactObject();
//        int cacheBorder = compact.getCacheBorder();
//        List<String> cacheOrders = compact.getColumnletOrder().subList(0, cacheBorder);
//        String directory = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_1_compact";
//        // varchar, varchar, int, boolean, varchar, varchar
//        String[] includedCols = new String[]{"formlevel2","visitformlevel1","overallplt","isnormalquery", "userfbid", "followonformlevel3"};
//        PixelsReaderOption option = new PixelsReaderOption();
//        option.rgRange(0, 16);
//        option.includeCols(includedCols);
//        option.skipCorruptRecords(true);
//        option.tolerantSchemaEvolution(true);
//        for (FileStatus fileStatus : fs.listStatus(new Path(directory)))
//        {
//            Path file = fileStatus.getPath();
//
//            // turn on caching
//            PixelsReader pixelsReaderWithCache = PixelsReaderImpl
//                    .newBuilder()
//                    .setFS(fs)
//                    .setPath(file)
//                    .setEnableCache(true)
//                    .setCacheOrder(cacheOrders)
//                    .setPixelsCacheReader(cacheReader)
//                    .build();
//
//            PixelsRecordReader pixelsRecordReaderWithCache = pixelsReaderWithCache.read(option);
//            VectorizedRowBatch rowBatchWithCache = pixelsRecordReaderWithCache.readBatch();
//            BytesColumnVector cache_c0 = (BytesColumnVector) rowBatchWithCache.cols[0];
//            BytesColumnVector cache_c1 = (BytesColumnVector) rowBatchWithCache.cols[1];
//            LongColumnVector cache_c2 = (LongColumnVector) rowBatchWithCache.cols[2];
//            LongColumnVector cache_c3 = (LongColumnVector) rowBatchWithCache.cols[3];
//            BytesColumnVector cache_c4 = (BytesColumnVector) rowBatchWithCache.cols[4];
//            BytesColumnVector cache_c5 = (BytesColumnVector) rowBatchWithCache.cols[5];
//
//            // turn off caching
//            PixelsReader pixelsReaderWithoutCache = PixelsReaderImpl
//                    .newBuilder()
//                    .setFS(fs)
//                    .setPath(file)
//                    .setEnableCache(false)
//                    .setCacheOrder(new ArrayList<>(0))
//                    .build();
//            PixelsRecordReader pixelsRecordReaderWithoutCache = pixelsReaderWithoutCache.read(option);
//            VectorizedRowBatch rowBatchWithoutCache = pixelsRecordReaderWithoutCache.readBatch();
//            BytesColumnVector c0 = (BytesColumnVector) rowBatchWithoutCache.cols[0];
//            BytesColumnVector c1 = (BytesColumnVector) rowBatchWithoutCache.cols[1];
//            LongColumnVector c2 = (LongColumnVector) rowBatchWithoutCache.cols[2];
//            LongColumnVector c3 = (LongColumnVector) rowBatchWithoutCache.cols[3];
//            BytesColumnVector c4 = (BytesColumnVector) rowBatchWithoutCache.cols[4];
//            BytesColumnVector c5 = (BytesColumnVector) rowBatchWithoutCache.cols[5];
//
//            // validate
//            assert rowBatchWithCache.size == rowBatchWithoutCache.size;
//            assertArrayEquals(cache_c0.isNull, c0.isNull);
//            assertArrayEquals(cache_c1.isNull, c1.isNull);
//            assertArrayEquals(cache_c2.isNull, c2.isNull);
//            assertArrayEquals(cache_c3.isNull, c3.isNull);
//            assertArrayEquals(cache_c4.isNull, c4.isNull);
//            assertArrayEquals(cache_c5.isNull, c5.isNull);
//            for (int i = 0; i < rowBatchWithCache.size; i++)
//            {
//                assertEquals(c0.toString(i), cache_c0.toString(i));
//                assertEquals(c1.toString(i), cache_c1.toString(i));
//                assertEquals(c2.vector[i], cache_c2.vector[i]);
//                assertEquals(c3.vector[i], cache_c3.vector[i]);
//                assertEquals(c4.toString(i), cache_c4.toString(i));
//                assertEquals(c5.toString(i), cache_c5.toString(i));
//            }
//        }
    }
}
