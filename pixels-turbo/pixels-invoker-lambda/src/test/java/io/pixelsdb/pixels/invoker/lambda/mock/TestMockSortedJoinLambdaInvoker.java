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
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.SortedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.junit.Test;

import java.util.Arrays;

public class TestMockSortedJoinLambdaInvoker
{
    @Test
    public void testOrdersLineitem()
    {

        SortedJoinInput joinInput = new SortedJoinInput();
        joinInput.setTransId(123456);
        SortedTableInfo leftTableInfo = new SortedTableInfo();
        leftTableInfo.setTableName("orders");
        leftTableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        leftTableInfo.setKeyColumnIds(new int[]{0});
        leftTableInfo.setInputFiles(Arrays.asList(
                "/home/ubuntu/test/pixels-lambda-test/orders_6190",
                "/home/ubuntu/test/pixels-lambda-test/orders_6191"));
        leftTableInfo.setParallelism(8);
        leftTableInfo.setBase(false);
        leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.file, null, null, null, null));
        joinInput.setSmallTable(leftTableInfo);

        SortedTableInfo rightTableInfo = new SortedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        rightTableInfo.setKeyColumnIds(new int[]{0});
        rightTableInfo.setInputFiles(Arrays.asList(
                "/home/ubuntu/test/pixels-lambda-test/lineitem_6002",
                "/home/ubuntu/test/pixels-lambda-test/lineitem_6003"));
        rightTableInfo.setParallelism(2);
        rightTableInfo.setBase(false);
        rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.file, null, null, null, null));
        joinInput.setLargeTable(rightTableInfo);

        SortedJoinInfo joinInfo = new SortedJoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setSmallColumnAlias(new String[]{"o_orderkey","o_custkey", "o_orderstatus", "o_orderdate"});
        joinInfo.setLargeColumnAlias(new String[]{"l_orderkey","l_partkey", "l_extendedprice", "l_discount"});
        joinInfo.setSmallProjection(new boolean[]{true, true, true, true});
        joinInfo.setLargeProjection(new boolean[]{true, true, true, true});
        joinInput.setJoinInfo(joinInfo);

        joinInput.setOutput(new MultiOutputInfo("/home/ubuntu/test/pixels-lambda-test/join",
                new StorageInfo(Storage.Scheme.file, null, null, null, null),
                true, Arrays.asList("sorted_join_lineitem_orders_0"))); // force one file currently

        System.out.println(JSON.toJSONString(joinInput));
        MockSortedJoinWorker mockSortedJoinWorker = new MockSortedJoinWorker();
        JoinOutput output = mockSortedJoinWorker.process(joinInput);
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }
}
