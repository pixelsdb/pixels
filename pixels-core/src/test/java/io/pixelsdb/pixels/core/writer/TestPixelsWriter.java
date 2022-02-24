/*
 * Copyright 2017-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.cache.PixelsCacheConfig;
import io.pixelsdb.pixels.cache.PixelsCacheKey;
import io.pixelsdb.pixels.cache.PixelsCacheWriter;
import io.pixelsdb.pixels.cache.PixelsPhysicalReader;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 * @author hank
 */
public class TestPixelsWriter
{

    @Test
    public void testWriterWithNull()
    {
        String filePath = TestParams.filePath;

        // schema: struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("file");
            TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            LongColumnVector a = (LongColumnVector) rowBatch.cols[0];              // int
            DoubleColumnVector b = (DoubleColumnVector) rowBatch.cols[1];          // float
            DoubleColumnVector c = (DoubleColumnVector) rowBatch.cols[2];          // double
            TimestampColumnVector d = (TimestampColumnVector) rowBatch.cols[3];    // timestamp
            ByteColumnVector e = (ByteColumnVector) rowBatch.cols[4];              // boolean
            DateColumnVector f = (DateColumnVector) rowBatch.cols[5];              // date
            TimeColumnVector g = (TimeColumnVector) rowBatch.cols[6];              // time
            BinaryColumnVector h = (BinaryColumnVector) rowBatch.cols[7];          // string

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(TestParams.pixelStride)
                            .setRowGroupSize(TestParams.rowGroupSize)
                            .setStorage(storage)
                            .setFilePath(filePath)
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
                    d.times[row] = 0;
                    d.nanos[row] = 0;
                    e.isNull[row] = true;
                    e.vector[row] = 0;
                    f.isNull[row] = true;
                    f.dates[row] = 0;
                    g.isNull[row] = true;
                    g.times[row] = 0;
                    h.isNull[row] = true;
                    h.vector[row] = new byte[0];
                }
                else
                {
                    a.vector[row] = i;
                    a.isNull[row] = false;
                    b.vector[row] = Float.floatToIntBits(i * 3.1415f);
                    b.isNull[row] = false;
                    c.vector[row] = Double.doubleToLongBits(i * 3.14159d);
                    c.isNull[row] = false;
                    d.set(row, timestamp);
                    d.isNull[row] = false;
                    e.vector[row] = (byte) (i % 100 > 25 ? 1 : 0);
                    e.isNull[row] = false;
                    f.set(row, new Date(System.currentTimeMillis()));
                    f.isNull[row] = false;
                    g.set(row, new Time(System.currentTimeMillis()));
                    g.isNull[row] = false;
                    h.setVal(row, String.valueOf(i).getBytes());
                    h.isNull[row] = false;
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

        // schema: struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("hdfs");
            TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            LongColumnVector a = (LongColumnVector) rowBatch.cols[0];              // int
            DoubleColumnVector b = (DoubleColumnVector) rowBatch.cols[1];          // float
            DoubleColumnVector c = (DoubleColumnVector) rowBatch.cols[2];          // double
            TimestampColumnVector d = (TimestampColumnVector) rowBatch.cols[3];    // timestamp
            LongColumnVector e = (LongColumnVector) rowBatch.cols[4];              // boolean
            BinaryColumnVector z = (BinaryColumnVector) rowBatch.cols[5];            // string

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(TestParams.pixelStride)
                            .setRowGroupSize(TestParams.rowGroupSize)
                            .setStorage(storage)
                            .setFilePath(filePath)
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
                b.vector[row] = Float.floatToIntBits(i * 3.1415f);
                b.isNull[row] = false;
                c.vector[row] = Double.doubleToLongBits(i * 3.14159d);
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
        String[] cols = {"a", "b", "c", "d", "e", "f", "g", "h"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);
        option.rgRange(0, 1);

        VectorizedRowBatch rowBatch;
        PixelsReader pixelsReader;

        try
        {
            Storage storage = StorageFactory.Instance().getStorage("file");
            String path = TestParams.filePath;
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(path)
                    .setEnableCache(false)
                    .setCacheOrder(new ArrayList<>())
                    .setPixelsCacheReader(null)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
            PixelsRecordReader recordReader = pixelsReader.read(option);
            rowBatch = recordReader.readBatch();
            LongColumnVector acv = (LongColumnVector) rowBatch.cols[0];
            DoubleColumnVector bcv = (DoubleColumnVector) rowBatch.cols[1];
            DoubleColumnVector ccv = (DoubleColumnVector) rowBatch.cols[2];
            TimestampColumnVector dcv = (TimestampColumnVector) rowBatch.cols[3];
            ByteColumnVector ecv = (ByteColumnVector) rowBatch.cols[4];
            DateColumnVector fcv = (DateColumnVector) rowBatch.cols[5];
            TimeColumnVector gcv = (TimeColumnVector) rowBatch.cols[6];
            BinaryColumnVector hcv = (BinaryColumnVector) rowBatch.cols[7];
            for (int i = 0, j = 0; i < rowBatch.size; ++i)
            {
                if (fcv.isNull[i])
                {
                    System.out.println("null");
                }
                else
                {
                    System.out.println(fcv.asScratchDate(j++));
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testReadTpchNation()
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);
        option.rgRange(0, 1);

        VectorizedRowBatch rowBatch;
        PixelsReader pixelsReader;

        try
        {
            Storage storage = StorageFactory.Instance().getStorage("file");
            String path = TestParams.filePath;
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(path)
                    .setEnableCache(false)
                    .setCacheOrder(new ArrayList<>())
                    .setPixelsCacheReader(null)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
            PixelsRecordReader recordReader = pixelsReader.read(option);
            rowBatch = recordReader.readBatch();
            LongColumnVector nationkeyVector = (LongColumnVector) rowBatch.cols[0];
            BinaryColumnVector nameVector = (BinaryColumnVector) rowBatch.cols[1];
            LongColumnVector regionkeyVector = (LongColumnVector) rowBatch.cols[2];
            BinaryColumnVector commentVector = (BinaryColumnVector) rowBatch.cols[3];
            for (int i = 0; i < rowBatch.size; ++i)
            {
                String name = new String(nameVector.vector[i], nameVector.start[i], nameVector.lens[i]);
                String comment = new String(commentVector.vector[i], commentVector.start[i], commentVector.lens[i]);
                System.out.println(nationkeyVector.vector[i] + ", " + name + ", " + regionkeyVector.vector[i] + ", " + comment);
            }
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
            Storage storage = StorageFactory.Instance().getStorage("hdfs");
            PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
            PixelsCacheWriter cacheWriter =
                    PixelsCacheWriter.newBuilder()
                            .setCacheLocation("/Users/Jelly/Desktop/pixels.cache")
                            .setCacheSize(1024 * 1024 * 1024L)
                            .setIndexLocation("/Users/Jelly/Desktop/pixels.index")
                            .setIndexSize(1024 * 1024 * 1024L)
                            .setOverwrite(true)
                            .setCacheConfig(cacheConfig)
                            .build();
            String directory = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_1_compact";
            long cacheLength = 0L;
            List<Status> fileStatuses = storage.listStatus(directory);
            MetadataService metadataService = new MetadataService("dbiir10", 18888);
            Layout layout = metadataService.getLayout("pixels", "test_105", 0);
            Compact compact = layout.getCompactObject();
            int cacheBorder = compact.getCacheBorder();
            List<String> cacheOrders = compact.getColumnletOrder().subList(0, cacheBorder);
            long startNano = System.nanoTime();
            // write cache
            for (Status fileStatus : fileStatuses)
            {
                String file = fileStatus.getPath();
                PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file);
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
                    PixelsCacheKey cacheKey = new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), cacheRGId, cacheColId);
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
            metadataService.shutdown();
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
//            BinaryColumnVector cache_c0 = (BinaryColumnVector) rowBatchWithCache.cols[0];
//            BinaryColumnVector cache_c1 = (BinaryColumnVector) rowBatchWithCache.cols[1];
//            LongColumnVector cache_c2 = (LongColumnVector) rowBatchWithCache.cols[2];
//            LongColumnVector cache_c3 = (LongColumnVector) rowBatchWithCache.cols[3];
//            BinaryColumnVector cache_c4 = (BinaryColumnVector) rowBatchWithCache.cols[4];
//            BinaryColumnVector cache_c5 = (BinaryColumnVector) rowBatchWithCache.cols[5];
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
//            BinaryColumnVector c0 = (BinaryColumnVector) rowBatchWithoutCache.cols[0];
//            BinaryColumnVector c1 = (BinaryColumnVector) rowBatchWithoutCache.cols[1];
//            LongColumnVector c2 = (LongColumnVector) rowBatchWithoutCache.cols[2];
//            LongColumnVector c3 = (LongColumnVector) rowBatchWithoutCache.cols[3];
//            BinaryColumnVector c4 = (BinaryColumnVector) rowBatchWithoutCache.cols[4];
//            BinaryColumnVector c5 = (BinaryColumnVector) rowBatchWithoutCache.cols[5];
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
