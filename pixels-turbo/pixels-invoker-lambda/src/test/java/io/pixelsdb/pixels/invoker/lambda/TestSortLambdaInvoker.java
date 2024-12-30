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
package io.pixelsdb.pixels.invoker.lambda;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.SortInput;
import io.pixelsdb.pixels.planner.plan.physical.output.SortOutput;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class TestSortLambdaInvoker
{
    @Test
    public void testOrders() throws ExecutionException, InterruptedException
    {
        for (int i = 1500; i <= 1501; ++i)
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
            SortInput input = new SortInput();
            input.setTransId(123456);
            ScanTableInfo tableInfo = new ScanTableInfo();
            tableInfo.setTableName("orders");
            String prefix = new String("pixels-turbo-intermediate/zhujiaxuan/test/data/orders/v-0-compact/20241226131132_");
            tableInfo.setInputSplits(Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 0, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 4, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 8, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 12, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 16, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 20, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 24, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 28, 4)))));
            tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
            tableInfo.setFilter(filter);
            tableInfo.setBase(true);
            tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
            input.setTableInfo(tableInfo);
            input.setProjection(new boolean[]{true, true, true, true});
            input.setKeyColumnIds(new int[]{0});
            input.setOutput(new OutputInfo("pixels-turbo-intermediate/zhujiaxuan/test/result/orders_" + i,
                    new StorageInfo(Storage.Scheme.s3, null, null, null, null), true));

            System.out.println(JSON.toJSONString(input));

            SortOutput output = (SortOutput) InvokerFactory.Instance()
                    .getInvoker(WorkerType.SORT).invoke(input).get();
            System.out.println(output.getOutputs().size());
            System.out.println(output.getOutputs());
            System.out.println(Joiner.on(",").join(output.getOutputs()));
        }
    }

    @Test
    public void testLineitem() throws ExecutionException, InterruptedException
    {
        for (int i = 7704; i < 7706; ++i)
        {
            String filter =
                    "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
            SortInput input = new SortInput();
            input.setTransId(123456);
            ScanTableInfo tableInfo = new ScanTableInfo();
            tableInfo.setTableName("lineitem");
            String prefix = new String("pixels-turbo-intermediate/zhujiaxuan/test/data/lineitem/v-0-compact/20241226132606_");
            tableInfo.setInputSplits(Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 0, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 4, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 8, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 12, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 16, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 20, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 24, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 28, 4)))));
            tableInfo.setFilter(filter);
            tableInfo.setBase(true);
            tableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
            tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
            input.setTableInfo(tableInfo);
            input.setProjection(new boolean[]{true, true, true, true});
            input.setKeyColumnIds(new int[]{0});
            input.setOutput(new OutputInfo("pixels-turbo-intermediate/zhujiaxuan/test/result/lineitem_" + i,
                    new StorageInfo(Storage.Scheme.s3, null, null, null, null), true));

            System.out.println(JSON.toJSONString(input));

            SortOutput output = (SortOutput) InvokerFactory.Instance()
                    .getInvoker(WorkerType.SORT).invoke(input).get();
            System.out.println(output.getOutputs().size());
            System.out.println(Joiner.on(",").join(output.getOutputs()));
        }
    }
}
