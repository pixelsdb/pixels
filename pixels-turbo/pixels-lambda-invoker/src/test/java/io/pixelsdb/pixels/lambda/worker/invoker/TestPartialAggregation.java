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
import io.pixelsdb.pixels.turbo.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.turbo.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.turbo.planner.plan.physical.output.ScanOutput;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @date 08/07/2022
 */
public class TestPartialAggregation
{
    @Before
    public void registerInvokers()
    {
        InvokerFactory.Instance().registerInvokers(new LambdaInvokerProducer());
    }

    @Test
    public void testScanPartialAggregation() throws ExecutionException, InterruptedException
    {
        String filter =
                "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
        ScanInput scanInput = new ScanInput();
        scanInput.setQueryId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("orders");
        tableInfo.setColumnsToRead(new String[] {"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        tableInfo.setFilter(filter);
        tableInfo.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20220313171727_7.compact.pxl", 28, 4)))));
        scanInput.setTableInfo(tableInfo);
        scanInput.setPartialAggregationPresent(true);
        PartialAggregationInfo aggregationInfo = new PartialAggregationInfo();
        aggregationInfo.setGroupKeyColumnAlias(new String[] {"o_orderstatus_2", "o_orderdate_3"});
        aggregationInfo.setGroupKeyColumnIds(new int[] {2, 3});
        aggregationInfo.setAggregateColumnIds(new int[] {0});
        aggregationInfo.setResultColumnAlias(new String[] {"sum_o_orderkey_0"});
        aggregationInfo.setResultColumnTypes(new String[] {"bigint"});
        aggregationInfo.setFunctionTypes(new FunctionType[] {FunctionType.SUM});
        scanInput.setPartialAggregationInfo(aggregationInfo);
        scanInput.setOutput(new OutputInfo("pixels-lambda-test/orders_partial_aggr_7", false,
                new StorageInfo(Storage.Scheme.s3, null, null, null), true));

        System.out.println(JSON.toJSONString(scanInput));

        ScanOutput output = (ScanOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.SCAN).invoke(scanInput).get();
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }
}
