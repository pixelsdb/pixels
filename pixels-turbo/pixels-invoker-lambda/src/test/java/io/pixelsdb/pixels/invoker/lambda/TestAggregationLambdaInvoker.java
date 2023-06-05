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
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregatedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @create 2022-07-08
 */
public class TestAggregationLambdaInvoker
{
    @Test
    public void testOrders() throws ExecutionException, InterruptedException
    {
        AggregationInput aggregationInput = new AggregationInput();
        aggregationInput.setTransId(123456);
        AggregatedTableInfo aggregatedTableInfo = new AggregatedTableInfo();
        aggregatedTableInfo.setParallelism(8);
        aggregatedTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3,
                null, null, null, null));
        aggregatedTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/unit_tests/orders_partial_aggr_0",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_1",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_2",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_3",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_4",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_5",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_6",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_7"));
        aggregatedTableInfo.setColumnsToRead(new String[] {"sum_o_orderkey_0", "o_orderstatus_2", "o_orderdate_3"});
        aggregatedTableInfo.setBase(false);
        aggregatedTableInfo.setTableName("aggregate_orders");
        aggregationInput.setAggregatedTableInfo(aggregatedTableInfo);
        AggregationInfo aggregationInfo = new AggregationInfo();
        aggregationInfo.setGroupKeyColumnIds(new int[] {1, 2});
        aggregationInfo.setAggregateColumnIds(new int[] {0});
        aggregationInfo.setGroupKeyColumnNames(new String[] {"o_orderstatus", "o_orderdate"});
        aggregationInfo.setGroupKeyColumnProjection(new boolean[] {true, true});
        aggregationInfo.setResultColumnNames(new String[] {"sum_o_orderkey"});
        aggregationInfo.setResultColumnTypes(new String[] {"bigint"});
        aggregationInfo.setFunctionTypes(new FunctionType[] {FunctionType.SUM});
        aggregationInput.setAggregationInfo(aggregationInfo);
        aggregationInput.setOutput(new OutputInfo("pixels-lambda-test/unit_tests/orders_final_aggr", false,
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), true));

        System.out.println(JSON.toJSONString(aggregationInput));

        AggregationOutput output = (AggregationOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.AGGREGATION).invoke(aggregationInput).get();
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }
}
