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
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.executor.lambda.BroadcastJoinInvoker;
import io.pixelsdb.pixels.executor.lambda.domain.*;
import io.pixelsdb.pixels.executor.lambda.input.BroadcastJoinInput;
import io.pixelsdb.pixels.executor.lambda.output.JoinOutput;
import io.pixelsdb.pixels.executor.predicate.Bound;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import io.pixelsdb.pixels.executor.predicate.Filter;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static io.pixelsdb.pixels.executor.predicate.Bound.Type.INCLUDED;

/**
 * @author hank
 * @date 15/05/2022
 */
public class TestBroadcastJoinInvoker
{
    @Test
    public void testPartLineitem() throws ExecutionException, InterruptedException
    {
        String leftFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"part\"," +
                "\"columnFilters\":{2:{\"columnName\":\"p_size\",\"columnType\":\"INT\"," +
                "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"onlyNull\\\":false," +
                "\\\"ranges\\\":[],\\\"discreteValues\\\":[{" +
                "\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":49}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":14}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":23}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":45}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":19}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":3}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":36}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":9}]}\"}}}";

        // leftFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"part\",\"columnFilters\":{}}";
        String rightFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";

        BroadcastJoinInput joinInput = new BroadcastJoinInput();
        joinInput.setQueryId(123456);

        BroadCastJoinTableInfo leftTable = new BroadCastJoinTableInfo();
        leftTable.setColumnsToRead(new String[]{"p_partkey", "p_name", "p_size"});
        leftTable.setKeyColumnIds(new int[]{0});
        leftTable.setTableName("part");
        leftTable.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20220313172545_0.compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20220313172545_0.compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20220313172545_0.compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20220313172545_0.compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20220313172545_0.compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20220313172545_0.compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20220313172545_0.compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20220313172545_0.compact.pxl", 28, 4)))));
        leftTable.setFilter(leftFilter);
        joinInput.setLeftTable(leftTable);

        BroadCastJoinTableInfo rightTable = new BroadCastJoinTableInfo();
        rightTable.setColumnsToRead(new String[]{"l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        rightTable.setKeyColumnIds(new int[]{1});
        rightTable.setTableName("lineitem");
        rightTable.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 28, 4)))));
        rightTable.setFilter(rightFilter);
        joinInput.setRightTable(rightTable);

        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setResultColumns(new String[]{"p_partkey", "p_name", "p_size",
                "l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        joinInfo.setPostPartition(true);
        joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {3}, 100));
        joinInput.setJoinInfo(joinInfo);
        joinInput.setOutput(new MultiOutputInfo("pixels-lambda/", Storage.Scheme.minio,
                "http://172.31.32.193:9000", "lambda", "password", true,
                Arrays.asList("broadcast-join-0","broadcast-join-1","broadcast-join-2","broadcast-join-3",
                        "broadcast-join-4","broadcast-join-5","broadcast-join-6","broadcast-join-7")));

        System.out.println(JSON.toJSONString(joinInput));
        JoinOutput output = BroadcastJoinInvoker.invoke(joinInput).get();
        System.out.println(output.getOutputs().size());
        for (int i = 0; i < output.getOutputs().size(); ++i)
        {
            System.out.println(output.getOutputs().get(i));
            System.out.println(output.getRowGroupNums().get(i));
            System.out.println();
        }
    }

    @Test
    public void testSerFilter()
    {
        ArrayList<Bound<Long>> discreteValues = new ArrayList<>();
        discreteValues.add(new Bound<>(INCLUDED, 49L));
        discreteValues.add(new Bound<>(INCLUDED, 14L));
        discreteValues.add(new Bound<>(INCLUDED, 23L));
        discreteValues.add(new Bound<>(INCLUDED, 45L));
        discreteValues.add(new Bound<>(INCLUDED, 19L));
        discreteValues.add(new Bound<>(INCLUDED, 3L));
        discreteValues.add(new Bound<>(INCLUDED, 36L));
        discreteValues.add(new Bound<>(INCLUDED, 9L));
        ColumnFilter<Long> columnFilter = new ColumnFilter<Long>("p_size", TypeDescription.Category.INT,
                new Filter<>(Long.TYPE, new ArrayList<>(), discreteValues, false, false, false, false));
        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        columnFilters.put(2, columnFilter);
        TableScanFilter filter = new TableScanFilter("tpch", "lineitem", columnFilters);
        System.out.println(JSON.toJSONString(filter));
    }

    @Test
    public void testDeFilter()
    {
        String filter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\"," +
                "\"columnFilters\":{2:{\"columnName\":\"p_size\",\"columnType\":\"INT\"," +
                "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"onlyNull\\\":false," +
                "\\\"ranges\\\":[],\\\"discreteValues\\\":[{" +
                "\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":49}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":14}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":23}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":45}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":19}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":3}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":36}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":9}]}\"}}}";
        TableScanFilter tableScanFilter = JSON.parseObject(filter, TableScanFilter.class);
        assert tableScanFilter.getColumnFilter(2).getFilter().getDiscreteValueCount() == 8;
    }
}
