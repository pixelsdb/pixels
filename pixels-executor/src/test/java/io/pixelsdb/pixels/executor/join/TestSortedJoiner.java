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
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TestSortedJoiner
{
    @Test
    public void test1() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReaderCustomer = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/customer/v-0-compact/20241214165238_15_compact.pxl")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        long transId = 123456;
        option.transId(transId);
        String[] customerCols = new String[]{"c_custkey", "c_name", "c_address",
                "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
        String customerSchema = new String("struct<c_custkey:bigint,c_name:varchar(25),c_address:varchar(40),c_nationkey:bigint,c_phone:char(15),c_acctbal:decimal(15,2),c_mktsegment:char(10),c_comment:varchar(117)>");
        option.includeCols(customerCols);
        option.rgRange(0, 4);
        PixelsRecordReader recordReaderCustomer = pixelsReaderCustomer.read(option);
        VectorizedRowBatch rowBatch;

        PixelsReader pixelsReaderNation = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/nation/v-0-compact/20241219065626_0_compact.pxl")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        String[] nationCols = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
        String nationSchema = new String("struct<n_nationkey:bigint,n_name:char(25),n_regionkey:bigint,n_comment:varchar(152)>");
        option.includeCols(nationCols);
        option.rgRange(0, -1);
        PixelsRecordReader recordReaderNation = pixelsReaderNation.read(option);

        SortedJoiner joiner = new SortedJoiner(JoinType.EQUI_INNER,
                TypeDescription.fromString(nationSchema),
                nationCols,
                new boolean[]{true, true, true, true},
                new int[]{0},
                TypeDescription.fromString(customerSchema),
                customerCols,
                new boolean[]{true, true, true, true, true, true, true, true},
                new int[]{3});

        do
        {
            rowBatch = recordReaderNation.readBatch(1000);
            if (rowBatch.size > 0)
            {
                joiner.populateLeftTable(rowBatch, 0); // 把rowbatch 插入到sorted left table。 怎么插入的时候转为Tuple
            }
        } while (!rowBatch.endOfFile);

        joiner.mergeLeftTable();
        ConcurrentLinkedQueue<VectorizedRowBatch> joinResult = new ConcurrentLinkedQueue<>();
        int joinedRows = 0;
        if (!joiner.sortedSmallTable.isEmpty())
        {
            do
            {
                rowBatch = recordReaderCustomer.readBatch(1000);
                if (rowBatch.size > 0)
                {
                    List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                    for (VectorizedRowBatch joined : joinedBatches)
                    {
                        joinResult.add(joined);
                        joinedRows += joined.size;
                    }
                }
            } while (!rowBatch.endOfFile);
        }

        System.out.println("joinedRows = " + joinedRows);
        recordReaderCustomer.close();
        recordReaderNation.close();
    }

    @Test
    public void test2() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReaderCustomer = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/customer/v-0-compact/20241214165238_15_compact.pxl")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        long transId = 123456;
        option.transId(transId);
        String[] customerCols = new String[]{"c_custkey", "c_name", "c_address",
                "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
        String customerSchema = new String("struct<c_custkey:bigint,c_name:varchar(25),c_address:varchar(40),c_nationkey:bigint,c_phone:char(15),c_acctbal:decimal(15,2),c_mktsegment:char(10),c_comment:varchar(117)>");
        option.includeCols(customerCols);
        option.rgRange(0, 4);
        PixelsRecordReader recordReaderCustomer = pixelsReaderCustomer.read(option);
        VectorizedRowBatch rowBatch;

        PixelsReader pixelsReaderNation = PixelsReaderImpl.newBuilder()
                .setPath("/home/ubuntu/test/nation/v-0-compact/20241219065626_0_compact.pxl")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        String[] nationCols = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
        String nationSchema = new String("struct<n_nationkey:bigint,n_name:char(25),n_regionkey:bigint,n_comment:varchar(152)>");
        option.includeCols(nationCols);
        option.rgRange(0, -1);
        PixelsRecordReader recordReaderNation = pixelsReaderNation.read(option);

        SortedJoiner joiner = new SortedJoiner(JoinType.EQUI_FULL,
                TypeDescription.fromString(nationSchema),
                nationCols,
                new boolean[]{true, true, true, true},
                new int[]{0},
                TypeDescription.fromString(customerSchema),
                customerCols,
                new boolean[]{true, true, true, true, true, true, true, true},
                new int[]{3});

        do
        {
            rowBatch = recordReaderNation.readBatch(1000);
            if (rowBatch.size > 0)
            {
                joiner.populateLeftTable(rowBatch, 0);
            }
        } while (!rowBatch.endOfFile);

        joiner.mergeLeftTable();
        ConcurrentLinkedQueue<VectorizedRowBatch> joinResult = new ConcurrentLinkedQueue<>();
        int joinedRows = 0;
        if (!joiner.sortedSmallTable.isEmpty())
        {
            do
            {
                rowBatch = recordReaderCustomer.readBatch(1000);
                if (rowBatch.size > 0)
                {
                    List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                    for (VectorizedRowBatch joined : joinedBatches)
                    {
                        joinResult.add(joined);
                        joinedRows += joined.size;
                    }
                }
            } while (!rowBatch.endOfFile);
        }
        System.out.println("joinedRows = " + joinedRows);


        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(joiner.joinedSchema)
                .setPixelStride(100)
                .setRowGroupSize(1000)
                .setStorage(storage)
                .setPath("/home/ubuntu/test/output/joinoutput")
                .setOverwrite(true)
                .setEncodingLevel(EncodingLevel.EL2)
                .setPartitioned(false).build();

        for (VectorizedRowBatch res : joinResult)
        {
            pixelsWriter.addRowBatch(res);
        }
        pixelsWriter.close();
        if (joiner.joinType == JoinType.EQUI_LEFT || joiner.joinType == JoinType.EQUI_FULL)
        {
            pixelsWriter = PixelsWriterImpl.newBuilder()
                    .setSchema(joiner.joinedSchema)
                    .setPixelStride(100)
                    .setRowGroupSize(1000)
                    .setStorage(storage)
                    .setPath("/home/ubuntu/test/output/joinoutput2")
                    .setOverwrite(true)
                    .setEncodingLevel(EncodingLevel.EL2)
                    .setPartitioned(false).build();
            joiner.writeLeftOuter(pixelsWriter, 1000);
            pixelsWriter.close();
        }
        recordReaderCustomer.close();
        recordReaderNation.close();
    }
}
