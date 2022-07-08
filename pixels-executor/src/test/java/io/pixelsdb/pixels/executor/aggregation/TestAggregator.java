/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.executor.aggregation;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.DateColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.dayToMillis;

/**
 * @author hank
 * @date 08/07/2022
 */
public class TestAggregator
{
    @Test
    public void test() throws IOException
    {
        long start = System.currentTimeMillis();
        String filePath = "/home/hank/Desktop/20220313083947_0.pxl";
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath(filePath)
                .setStorage(storage)
                .setPixelsFooterCache(new PixelsFooterCache())
                .setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[] {"o_custkey", "o_orderstatus", "o_orderdate", "o_shippriority"});
        PixelsRecordReader recordReader = pixelsReader.read(option);
        System.out.println("millis to read: " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        Aggregator aggregator = new Aggregator(1024, recordReader.getResultSchema(),
                new String[] {"o_orderstatus_1", "o_orderdate_2"},
                new int[] {1, 2}, new boolean[] {true, true}, new int[] {0},
                new String[] {"sum_o_custkey_3"}, new String[] {"bigint"},
                new FunctionType[] {FunctionType.SUM});
        VectorizedRowBatch rowBatch;
        do
        {
            rowBatch = recordReader.readBatch(1024);
            if (rowBatch.size > 0)
            {
                //aggregator.aggregate(rowBatch);
            }
        } while (!rowBatch.endOfFile);
        pixelsReader.close();
        System.out.println("millis to aggregate: " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();

        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(aggregator.getOutputSchema())
                .setStorage(storage)
                .setPath("/home/hank/Desktop/final_aggr.pxl")
                .setRowGroupSize(268435456)
                .setPixelStride(10000)
                .setEncoding(true)
                .setPartitioned(false)
                .setOverwrite(true).build();

        aggregator.writeAggrOutput(pixelsWriter);
        pixelsWriter.close();
        System.out.println("millis to write: " + (System.currentTimeMillis() - start));
    }

    @After
    public void after() throws IOException
    {
        String filePath = "/home/hank/Desktop/final_aggr.pxl";
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath(filePath)
                .setStorage(storage)
                .setPixelsFooterCache(new PixelsFooterCache())
                .setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[] {"o_orderstatus_1", "o_orderdate_2", "sum_o_custkey_3"});
        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        do
        {
            rowBatch = recordReader.readBatch(1024);
            for (int i = 0; i < rowBatch.size; ++i)
            {
                BinaryColumnVector c0 = (BinaryColumnVector) rowBatch.cols[0];
                DateColumnVector c1 = (DateColumnVector) rowBatch.cols[1];
                LongColumnVector c2 = (LongColumnVector) rowBatch.cols[2];
                System.out.println(new String(c0.vector[i], c0.start[i], c0.lens[i]) + ", "
                + (new Date(dayToMillis(c1.dates[i]))) + ", " + c2.vector[i]);
            }
        } while (!rowBatch.endOfFile);
    }
}
