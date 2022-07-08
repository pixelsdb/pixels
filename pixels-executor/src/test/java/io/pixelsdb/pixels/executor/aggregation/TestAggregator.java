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
        option.includeCols(new String[] {"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        PixelsRecordReader recordReader = pixelsReader.read(option);
        System.out.println("millis to read: " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        int num = 0;
        Aggregator aggregator = new Aggregator(1024, recordReader.getResultSchema(),
                new String[] {"o_custkey_1", "o_orderstatus_2", "o_orderdate_3"},
                new int[] {1, 2, 3}, new boolean[] {true, true, true}, new int[] {0},
                new String[] {"sum_o_orderkey_0"}, new String[] {"bigint"},
                new FunctionType[] {FunctionType.SUM});
        VectorizedRowBatch rowBatch;
        do
        {
            rowBatch = recordReader.readBatch(1024);
            if (rowBatch.size > 0)
            {
                num += rowBatch.size;
                aggregator.aggregate(rowBatch);
            }
        } while (!rowBatch.endOfFile);
        pixelsReader.close();
        System.out.println(num);
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
        option.includeCols(new String[] {"o_custkey_1", "o_orderstatus_2", "o_orderdate_3", "sum_o_orderkey_0"});
        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch(1024);
            for (int i = 0; i < rowBatch.size; ++i)
            {
                LongColumnVector c0 = (LongColumnVector) rowBatch.cols[0];
                BinaryColumnVector c1 = (BinaryColumnVector) rowBatch.cols[1];
                DateColumnVector c2 = (DateColumnVector) rowBatch.cols[2];
                LongColumnVector c3 = (LongColumnVector) rowBatch.cols[3];
                //System.out.println(c0.vector[i] + ", " + new String(c1.vector[i], c1.start[i], c1.lens[i]) + ", "
                //+ (new Date(dayToMillis(c2.dates[i]))) + ", " + c3.vector[i]);
                num++;
            }
        } while (!rowBatch.endOfFile);
        System.out.println(num);
        pixelsReader.close();
    }
}
