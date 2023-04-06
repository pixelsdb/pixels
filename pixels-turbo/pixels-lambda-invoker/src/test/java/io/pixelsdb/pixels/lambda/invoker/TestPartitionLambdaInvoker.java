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
package io.pixelsdb.pixels.lambda.invoker;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @date 13/05/2022
 */
public class TestPartitionLambdaInvoker
{
    @Before
    public void registerInvokers()
    {
        InvokerFactory.Instance().registerInvokers(new LambdaInvokerProducer());
    }

    @Test
    public void testOrders() throws ExecutionException, InterruptedException
    {
        String filter = 
                "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                "\"columnFilters\":{1:{\"columnName\":\"o_custkey\",\"columnType\":\"LONG\"," +
                "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                "\\\"discreteValues\\\":[]}\"}}}";
        PartitionInput input = new PartitionInput();
        input.setQueryId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("orders");
        tableInfo.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_6.compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_6.compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_6.compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_6.compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_6.compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_6.compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_6.compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_6.compact.pxl", 28, 4)))));
        tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        tableInfo.setFilter(filter);
        input.setTableInfo(tableInfo);
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setNumPartition(40);
        partitionInfo.setKeyColumnIds(new int[]{0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new OutputInfo("pixels-lambda-test/orders_part_6", false,
                new StorageInfo(Storage.Scheme.s3, null, null, null), true));

        System.out.println(JSON.toJSONString(input));

        PartitionOutput output = (PartitionOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.PARTITION).invoke(input).get();
        System.out.println(output.getPath());
        for (int hashValue : output.getHashValues())
        {
            System.out.println(hashValue);
        }
    }

    @Test
    public void testLineitem() throws ExecutionException, InterruptedException
    {
        String filter =
                "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
        PartitionInput input = new PartitionInput();
        input.setQueryId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("lineitem");
        tableInfo.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 28, 4)))));
        tableInfo.setFilter(filter);
        tableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        input.setTableInfo(tableInfo);
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setNumPartition(40);
        partitionInfo.setKeyColumnIds(new int[]{0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new OutputInfo("pixels-lambda-test/lineitem_part_0", false,
                new StorageInfo(Storage.Scheme.s3, null, null, null),true));

        System.out.println(JSON.toJSONString(input));

        PartitionOutput output = (PartitionOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.PARTITION).invoke(input).get();
        System.out.println(output.getPath());
        for (int hashValue : output.getHashValues())
        {
            System.out.println(hashValue);
        }
    }
}
