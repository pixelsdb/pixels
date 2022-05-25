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
import io.pixelsdb.pixels.executor.lambda.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @date 14/05/2022
 */
public class TestPartitionedJoinInvoker
{
    @Test
    public void testOrdersLineitem() throws ExecutionException, InterruptedException
    {
        Set<Integer> hashValues = new HashSet<>(40);
        for (int i = 0 ; i < 40; ++i)
        {
            hashValues.add(i);
        }

        PartitionedJoinInput joinInput = new PartitionedJoinInput();
        joinInput.setQueryId(123456);
        joinInput.setLeftCols(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        joinInput.setLeftKeyColumnIds(new int[]{0});
        joinInput.setLeftTableName("orders");
        joinInput.setLeftPartitioned(Arrays.asList(
                new PartitionOutput("pixels-lambda-test/orders_part_0", hashValues),
                new PartitionOutput("pixels-lambda-test/orders_part_1", hashValues),
                new PartitionOutput("pixels-lambda-test/orders_part_2", hashValues),
                new PartitionOutput("pixels-lambda-test/orders_part_3", hashValues),
                new PartitionOutput("pixels-lambda-test/orders_part_4", hashValues),
                new PartitionOutput("pixels-lambda-test/orders_part_5", hashValues),
                new PartitionOutput("pixels-lambda-test/orders_part_6", hashValues),
                new PartitionOutput("pixels-lambda-test/orders_part_7", hashValues)
        ));
        joinInput.setRightCols(new String[]{"l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        joinInput.setRightKeyColumnIds(new int[]{0});
        joinInput.setRightTableName("lineitem");
        PartitionOutput lineitemPartitioned1 = new PartitionOutput();
        lineitemPartitioned1.setPath("pixels-lambda-test/lineitem_part_0");
        lineitemPartitioned1.setHashValues(hashValues);
        PartitionOutput lineitemPartitioned2 = new PartitionOutput();
        lineitemPartitioned2.setPath("pixels-lambda-test/lineitem_part_1");
        lineitemPartitioned2.setHashValues(hashValues);
        joinInput.setRightPartitioned(Arrays.asList(lineitemPartitioned1, lineitemPartitioned2));
        joinInput.setNumPartition(40);
        joinInput.setHashValues(Arrays.asList(16));
        joinInput.setJoinType(JoinType.EQUI_LEFT);
        joinInput.setJoinedCols(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate",
                "l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        joinInput.setOutput(new ScanInput.OutputInfo("pixels-lambda/",
                "http://172.31.32.193:9000", "lambda", "password", true));

        System.out.println(JSON.toJSONString(joinInput));
        JoinOutput output = PartitionedJoinInvoker.invoke(joinInput).get();
        System.out.println(output.getOutputs().size());
        for (int i = 0; i < output.getOutputs().size(); ++i)
        {
            System.out.println(output.getOutputs().get(i));
            System.out.println(output.getRowGroupNums().get(i));
            System.out.println();
        }
    }
}
