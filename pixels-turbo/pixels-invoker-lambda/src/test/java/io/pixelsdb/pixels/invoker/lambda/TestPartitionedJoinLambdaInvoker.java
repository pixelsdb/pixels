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
package io.pixelsdb.pixels.invoker.lambda;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @create 2022-05-14
 */
public class TestPartitionedJoinLambdaInvoker
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
        joinInput.setTransId(123456);
        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("orders");
        leftTableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        leftTableInfo.setKeyColumnIds(new int[]{0});
        leftTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/unit_tests/orders_part_0",
                "pixels-lambda-test/unit_tests/orders_part_1",
                "pixels-lambda-test/unit_tests/orders_part_2",
                "pixels-lambda-test/unit_tests/orders_part_3",
                "pixels-lambda-test/unit_tests/orders_part_4",
                "pixels-lambda-test/unit_tests/orders_part_5",
                "pixels-lambda-test/unit_tests/orders_part_6",
                "pixels-lambda-test/unit_tests/orders_part_7"));
        leftTableInfo.setParallelism(8);
        leftTableInfo.setBase(false);
        leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
        joinInput.setSmallTable(leftTableInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        rightTableInfo.setKeyColumnIds(new int[]{0});
        rightTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/unit_tests/lineitem_part_0",
                "pixels-lambda-test/unit_tests/lineitem_part_1"));
        rightTableInfo.setParallelism(2);
        rightTableInfo.setBase(false);
        rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
        joinInput.setLargeTable(rightTableInfo);

        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setNumPartition(40);
        joinInfo.setHashValues(Arrays.asList(16));
        joinInfo.setSmallColumnAlias(new String[]{"o_custkey", "o_orderstatus", "o_orderdate"});
        joinInfo.setLargeColumnAlias(new String[]{"l_partkey", "l_extendedprice", "l_discount"});
        joinInfo.setSmallProjection(new boolean[]{false, true, true, true});
        joinInfo.setLargeProjection(new boolean[]{false, true, true, true});
        joinInfo.setPostPartition(true);
        joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {0}, 100));
        joinInput.setJoinInfo(joinInfo);

        joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/unit_tests/",
                new StorageInfo(Storage.Scheme.s3, null, null, null),
                true, Arrays.asList("partitioned_join_lineitem_orders_0"))); // force one file currently

        System.out.println(JSON.toJSONString(joinInput));
        JoinOutput output = (JoinOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.PARTITIONED_JOIN).invoke(joinInput).get();
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }
}
