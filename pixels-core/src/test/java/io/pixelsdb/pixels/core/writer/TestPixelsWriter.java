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
                            .setPath(filePath)
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
                            .setPath(filePath)
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
}
