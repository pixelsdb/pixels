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
package io.pixelsdb.pixels.lambda.worker.invoker;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @date 08/07/2022
 */
public class TestAggregationLambdaInvoker
{
    @Before
    public void registerInvokers()
    {
        InvokerFactory.Instance().registerInvokers(new LambdaInvokerProducer());
    }

    @Test
    public void testOrders() throws ExecutionException, InterruptedException
    {
        AggregationInput aggregationInput = new AggregationInput();
        aggregationInput.setQueryId(123456);
        aggregationInput.setParallelism(8);
        aggregationInput.setInputStorage(new StorageInfo(Storage.Scheme.s3, null, null, null));
        aggregationInput.setInputFiles(Arrays.asList(
                "pixels-lambda-test/orders_partial_aggr_0",
                "pixels-lambda-test/orders_partial_aggr_1",
                "pixels-lambda-test/orders_partial_aggr_2",
                "pixels-lambda-test/orders_partial_aggr_3",
                "pixels-lambda-test/orders_partial_aggr_4",
                "pixels-lambda-test/orders_partial_aggr_5",
                "pixels-lambda-test/orders_partial_aggr_6",
                "pixels-lambda-test/orders_partial_aggr_7"));
        aggregationInput.setGroupKeyColumnNames(new String[] {"o_orderstatus_2", "o_orderdate_3"});
        aggregationInput.setGroupKeyColumnProjection(new boolean[] {true, true});
        aggregationInput.setResultColumnNames(new String[] {"sum_o_orderkey_0"});
        aggregationInput.setResultColumnTypes(new String[] {"bigint"});
        aggregationInput.setFunctionTypes(new FunctionType[] {FunctionType.SUM});
        aggregationInput.setOutput(new OutputInfo("pixels-lambda-test/orders_final_aggr", false,
                new StorageInfo(Storage.Scheme.s3, null, null, null), true));

        System.out.println(JSON.toJSONString(aggregationInput));

        AggregationOutput output = (AggregationOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.AGGREGATION).invoke(aggregationInput).get();
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }
}
