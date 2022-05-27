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
package io.pixelsdb.pixels.executor.join;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.executor.lambda.PartitionInput;
import io.pixelsdb.pixels.executor.lambda.PartitionInvoker;
import io.pixelsdb.pixels.executor.lambda.PartitionOutput;
import io.pixelsdb.pixels.executor.lambda.ScanInput;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @date 13/05/2022
 */
public class TestPartitionInvoker
{
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
        ArrayList<ScanInput.InputInfo> inputs = new ArrayList<>();
        inputs.add(new ScanInput.InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 0, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 4, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 8, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 12, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 16, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 20, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 24, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 28, 4));
        input.setInputs(inputs);
        input.setQueryId(123456);
        input.setSplitSize(4);
        input.setCols(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        PartitionInput.PartitionInfo partitionInfo = new PartitionInput.PartitionInfo();
        partitionInfo.setNumParition(40);
        partitionInfo.setKeyColumnIds(new int[]{0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new PartitionInput.OutputInfo("pixels-lambda-test/orders_part_7", true));
        input.setFilter(filter);

        System.out.println(JSON.toJSONString(input));

        PartitionOutput output = PartitionInvoker.invoke(input).get();
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
        ArrayList<ScanInput.InputInfo> inputs = new ArrayList<>();
        inputs.add(new ScanInput.InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_1.compact.pxl", 0, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_1.compact.pxl", 4, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_1.compact.pxl", 8, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_1.compact.pxl", 12, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_1.compact.pxl", 16, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_1.compact.pxl", 20, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_1.compact.pxl", 24, 4));
        inputs.add(new ScanInput.InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_1.compact.pxl", 28, 4));
        input.setInputs(inputs);
        input.setQueryId(123456);
        input.setSplitSize(4);
        input.setCols(new String[]{"l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        PartitionInput.PartitionInfo partitionInfo = new PartitionInput.PartitionInfo();
        partitionInfo.setNumParition(40);
        partitionInfo.setKeyColumnIds(new int[]{0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new PartitionInput.OutputInfo("pixels-lambda-test/lineitem_part_1", true));
        input.setFilter(filter);

        System.out.println(JSON.toJSONString(input));

        PartitionOutput output = PartitionInvoker.invoke(input).get();
        System.out.println(output.getPath());
        for (int hashValue : output.getHashValues())
        {
            System.out.println(hashValue);
        }
    }
}
