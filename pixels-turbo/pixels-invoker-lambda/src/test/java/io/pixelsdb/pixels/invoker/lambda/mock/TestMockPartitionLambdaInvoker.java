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
package io.pixelsdb.pixels.invoker.lambda.mock;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class TestMockPartitionLambdaInvoker
{
    @Test
    public void testOrders() throws ExecutionException, InterruptedException
    {
        for (int i = 6190; i <= 6190; ++i)
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
            input.setTransId(123456);
            ScanTableInfo tableInfo = new ScanTableInfo();
            tableInfo.setTableName("orders");
            String prefix = new String("/home/ubuntu/test/orders/v-0-compact/20241225084427_");
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
            tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.file, null, null, null, null));
            input.setTableInfo(tableInfo);
            input.setProjection(new boolean[]{true, true, true, true});
            PartitionInfo partitionInfo = new PartitionInfo();
            partitionInfo.setNumPartition(4);
            partitionInfo.setKeyColumnIds(new int[]{0});
            input.setPartitionInfo(partitionInfo);
            input.setOutput(new OutputInfo("/home/ubuntu/test/pixels-lambda-test/" + i,
                    new StorageInfo(Storage.Scheme.file, null, null, null, null), true));

            System.out.println(JSON.toJSONString(input));
            MockPartitionWorker mockPartitionWorker = new MockPartitionWorker();

            PartitionOutput output = mockPartitionWorker.process(input);
            System.out.println(output.getOutputs().size());
            System.out.println(Joiner.on(",").join(output.getOutputs()));
            System.out.println(Joiner.on(",").join(output.getHashValues()));
        }
    }

}
