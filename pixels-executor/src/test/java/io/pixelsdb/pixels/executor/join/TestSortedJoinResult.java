/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.executor.join;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestSortedJoinResult
{
    @Test
    public void testOrdersSort() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/pixels-lambda-test/orders_6190")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(123455);
        option.includeCols(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        option.rgRange(0, 1);
        PixelsRecordReader recordReader = pixelsReader.read(option);

        VectorizedRowBatch rowBatch;
        List<VectorizedRowBatch> result = new ArrayList<>();

        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch();
            if (rowBatch.size > 0)
            {
                result.add(rowBatch);
                num += rowBatch.size;
            }
        } while (!rowBatch.endOfFile);

        System.out.println("after sort " + num);

    }

    @Test
    public void testLineitemSort() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/pixels-lambda-test/lineitem_6002")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(123455);
        option.includeCols(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        option.rgRange(0, 1);
        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        List<VectorizedRowBatch> result = new ArrayList<>();

        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch();
            if (rowBatch.size > 0)
            {
                result.add(rowBatch);
                num += rowBatch.size;
            }
        } while (!rowBatch.endOfFile);
        System.out.println("after sort " + num);
    }

    @Test
    public void testOrdersLineitemJoin() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/pixels-lambda-test/join/sorted_join_lineitem_orders_0")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(123455);
        option.includeCols(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate", "l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        option.rgRange(0, 1);
        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        List<VectorizedRowBatch> result = new ArrayList<>();

        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch();
            if (rowBatch.size > 0)
            {
                result.add(rowBatch);
                num += rowBatch.size;
            }
        } while (!rowBatch.endOfFile);

        System.out.println("after join " + num);
    }


    @Test
    public void testOrdersSort2() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("pixels-turbo-intermediate/zhujiaxuan/test/result/orders_1500")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(123455);
        option.includeCols(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        option.rgRange(0, 1);
        PixelsRecordReader recordReader = pixelsReader.read(option);

        VectorizedRowBatch rowBatch;
        List<VectorizedRowBatch> result = new ArrayList<>();

        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch();
            if (rowBatch.size > 0)
            {
                result.add(rowBatch);
                num += rowBatch.size;
            }
        } while (!rowBatch.endOfFile);

        System.out.println("after sort " + num);

    }

    @Test
    public void testLineitemSort2() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("pixels-turbo-intermediate/zhujiaxuan/test/result/lineitem_7704")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(123455);
        option.includeCols(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        option.rgRange(0, 1);
        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        List<VectorizedRowBatch> result = new ArrayList<>();

        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch();
            if (rowBatch.size > 0)
            {
                result.add(rowBatch);
                num += rowBatch.size;
            }
        } while (!rowBatch.endOfFile);

        System.out.println("after sort " + num);

    }

    @Test
    public void testOrdersLineitemJoin2() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("pixels-turbo-intermediate/zhujiaxuan/test/result/sorted_join_lineitem_orders")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(123455);
        option.includeCols(new String[]{"o_custkey", "o_orderstatus", "o_orderdate", "l_partkey", "l_extendedprice", "l_discount"});
        option.rgRange(0, 1);
        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        List<VectorizedRowBatch> result = new ArrayList<>();

        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch();
            if (rowBatch.size > 0)
            {
                result.add(rowBatch);
                num += rowBatch.size;
            }
        } while (!rowBatch.endOfFile);

        System.out.println("after join " + num);
    }

    @Test
    public void testOrdersSort3() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/pixels-lambda-test/froms3/orders_1500")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(123455);
        option.includeCols(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        option.rgRange(0, 1);
        PixelsRecordReader recordReader = pixelsReader.read(option);

        VectorizedRowBatch rowBatch;
        List<VectorizedRowBatch> result = new ArrayList<>();

        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch();
            if (rowBatch.size > 0)
            {
                result.add(rowBatch);
                num += rowBatch.size;
            }
        } while (!rowBatch.endOfFile);

        System.out.println("after sort " + num);

    }

    @Test
    public void testLineitemSort3() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/pixels-lambda-test/froms3/lineitem_7704")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(123455);
        option.includeCols(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        option.rgRange(0, 1);
        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        List<VectorizedRowBatch> result = new ArrayList<>();

        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch();
            if (rowBatch.size > 0)
            {
                result.add(rowBatch);
                num += rowBatch.size;
            }
        } while (!rowBatch.endOfFile);

        System.out.println("after sort " + num);

    }

    @Test
    public void testOrdersLineitemJoin3() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/pixels-lambda-test/froms3/sorted_join_lineitem_orders")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(123455);
        option.includeCols(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate", "l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        option.rgRange(0, 1);
        PixelsRecordReader recordReader = pixelsReader.read(option);
        VectorizedRowBatch rowBatch;
        List<VectorizedRowBatch> result = new ArrayList<>();

        int num = 0;
        do
        {
            rowBatch = recordReader.readBatch();
            if (rowBatch.size > 0)
            {
                result.add(rowBatch);
                num += rowBatch.size;
            }
        } while (!rowBatch.endOfFile);

        System.out.println("after join " + num);
    }

}

