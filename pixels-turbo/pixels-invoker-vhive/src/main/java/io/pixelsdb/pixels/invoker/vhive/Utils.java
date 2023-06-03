package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.*;

import java.util.*;
import java.util.function.BiFunction;

public class Utils
{
    public static AggregationInput genAggregationInput(StorageInfo storageInfo)
    {
        AggregationInput aggregationInput = new AggregationInput();
        aggregationInput.setTransId(123456);
        AggregatedTableInfo aggregatedTableInfo = new AggregatedTableInfo();
        aggregatedTableInfo.setParallelism(8);
        aggregatedTableInfo.setStorageInfo(storageInfo);
        aggregatedTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/unit_tests/orders_partial_aggr_0",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_1",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_2",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_3",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_4",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_5",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_6",
                "pixels-lambda-test/unit_tests/orders_partial_aggr_7"));
        aggregatedTableInfo.setColumnsToRead(new String[]{"sum_o_orderkey_0", "o_orderstatus_2", "o_orderdate_3"});
        aggregatedTableInfo.setBase(false);
        aggregatedTableInfo.setTableName("aggregate_orders");
        aggregationInput.setAggregatedTableInfo(aggregatedTableInfo);
        AggregationInfo aggregationInfo = new AggregationInfo();
        aggregationInfo.setGroupKeyColumnIds(new int[]{1, 2});
        aggregationInfo.setAggregateColumnIds(new int[]{0});
        aggregationInfo.setGroupKeyColumnNames(new String[]{"o_orderstatus", "o_orderdate"});
        aggregationInfo.setGroupKeyColumnProjection(new boolean[]{true, true});
        aggregationInfo.setResultColumnNames(new String[]{"sum_o_orderkey"});
        aggregationInfo.setResultColumnTypes(new String[]{"bigint"});
        aggregationInfo.setFunctionTypes(new FunctionType[]{FunctionType.SUM});
        aggregationInput.setAggregationInfo(aggregationInfo);
        aggregationInput.setOutput(new OutputInfo("pixels-lambda-test/unit_tests/orders_final_aggr", false,
                storageInfo, true));
        return aggregationInput;
    }

    public static BroadcastChainJoinInput genBroadcastChainJoinInput(StorageInfo storageInfo)
    {
        String json = "{\"chainJoinInfos\":[{\"joinType\":\"EQUI_INNER\",\"keyColumnIds\":[0],\"largeColumnAlias\":[\"n_nationkey_0\"],\"largeProjection\":[true,false],\"postPartition\":false,\"smallColumnAlias\":[],\"smallProjection\":[false,false]},{\"joinType\":\"EQUI_INNER\",\"keyColumnIds\":[0],\"largeColumnAlias\":[\"c_custkey_0\"],\"largeProjection\":[true,false],\"postPartition\":false,\"smallColumnAlias\":[],\"smallProjection\":[false]},{\"joinType\":\"EQUI_INNER\",\"keyColumnIds\":[0],\"largeColumnAlias\":[\"o_orderkey_0\",\"o_orderdate_2\"],\"largeProjection\":[true,false,true],\"postPartition\":false,\"smallColumnAlias\":[],\"smallProjection\":[false]}],\"chainTables\":[{\"base\":true,\"columnsToRead\":[\"r_regionkey\",\"r_name\"],\"filter\":\"{\\\"schemaName\\\":\\\"tpch\\\",\\\"tableName\\\":\\\"region\\\",\\\"columnFilters\\\":{1:{\\\"columnName\\\":\\\"r_name\\\",\\\"columnType\\\":\\\"CHAR\\\",\\\"filterJson\\\":\\\"{\\\\\\\"javaType\\\\\\\":\\\\\\\"java.lang.String\\\\\\\",\\\\\\\"isAll\\\\\\\":false,\\\\\\\"isNone\\\\\\\":false,\\\\\\\"allowNull\\\\\\\":false,\\\\\\\"onlyNull\\\\\\\":false,\\\\\\\"ranges\\\\\\\":[],\\\\\\\"discreteValues\\\\\\\":[{\\\\\\\"type\\\\\\\":\\\\\\\"INCLUDED\\\\\\\",\\\\\\\"value\\\\\\\":\\\\\\\"AMERICA\\\\\\\"}]}\\\"}}}\",\"inputSplits\":[{\"inputInfos\":[{\"path\":\"minio://pixels-tpch/1g/region/v-0-order/20230513184540_20.pxl\",\"rgLength\":1,\"rgStart\":0}]}],\"keyColumnIds\":[0],\"storageInfo\":{\"accessKey\":\"input-ak-dummy\",\"endpoint\":\"input-endpoint-dummy\",\"scheme\":\"minio\",\"secretKey\":\"input-sk-dummy\"},\"tableName\":\"region\"},{\"base\":true,\"columnsToRead\":[\"n_nationkey\",\"n_regionkey\"],\"filter\":\"{\\\"schemaName\\\":\\\"tpch\\\",\\\"tableName\\\":\\\"nation\\\",\\\"columnFilters\\\":{}}\",\"inputSplits\":[{\"inputInfos\":[{\"path\":\"minio://pixels-tpch/1g/nation/v-0-order/20230513184436_12.pxl\",\"rgLength\":1,\"rgStart\":0}]}],\"keyColumnIds\":[1],\"storageInfo\":{\"$ref\":\"$.chainTables[0].storageInfo\"},\"tableName\":\"nation\"},{\"base\":true,\"columnsToRead\":[\"c_custkey\",\"c_nationkey\"],\"filter\":\"{\\\"schemaName\\\":\\\"tpch\\\",\\\"tableName\\\":\\\"customer\\\",\\\"columnFilters\\\":{}}\",\"inputSplits\":[{\"inputInfos\":[{\"path\":\"minio://pixels-tpch/1g/customer/v-0-order/20230513184339_0.pxl\",\"rgLength\":1,\"rgStart\":0}]}],\"keyColumnIds\":[1],\"storageInfo\":{\"$ref\":\"$.chainTables[0].storageInfo\"},\"tableName\":\"customer\"},{\"base\":true,\"columnsToRead\":[\"o_orderkey\",\"o_custkey\",\"o_orderdate\"],\"filter\":\"{\\\"schemaName\\\":\\\"tpch\\\",\\\"tableName\\\":\\\"orders\\\",\\\"columnFilters\\\":{2:{\\\"columnName\\\":\\\"o_orderdate\\\",\\\"columnType\\\":\\\"DATE\\\",\\\"filterJson\\\":\\\"{\\\\\\\"javaType\\\\\\\":\\\\\\\"int\\\\\\\",\\\\\\\"isAll\\\\\\\":false,\\\\\\\"isNone\\\\\\\":false,\\\\\\\"allowNull\\\\\\\":false,\\\\\\\"onlyNull\\\\\\\":false,\\\\\\\"ranges\\\\\\\":[{\\\\\\\"lowerBound\\\\\\\":{\\\\\\\"type\\\\\\\":\\\\\\\"INCLUDED\\\\\\\",\\\\\\\"value\\\\\\\":9131},\\\\\\\"upperBound\\\\\\\":{\\\\\\\"type\\\\\\\":\\\\\\\"INCLUDED\\\\\\\",\\\\\\\"value\\\\\\\":9861}}],\\\\\\\"discreteValues\\\\\\\":[]}\\\"}}}\",\"inputSplits\":[{\"inputInfos\":[{\"path\":\"minio://pixels-tpch/1g/orders/v-0-order/20230513184457_13.pxl\",\"rgLength\":1,\"rgStart\":0},{\"path\":\"minio://pixels-tpch/1g/orders/v-0-order/20230513184459_14.pxl\",\"rgLength\":1,\"rgStart\":0},{\"path\":\"minio://pixels-tpch/1g/orders/v-0-order/20230513184502_15.pxl\",\"rgLength\":1,\"rgStart\":0}]}],\"keyColumnIds\":[1],\"storageInfo\":{\"$ref\":\"$.chainTables[0].storageInfo\"},\"tableName\":\"orders\"}],\"joinInfo\":{\"joinType\":\"EQUI_INNER\",\"largeColumnAlias\":[],\"largeProjection\":[false,false],\"postPartition\":false,\"smallColumnAlias\":[\"o_orderdate_5\",\"n_name_4\",\"l_extendedprice_2\",\"l_discount_3\"],\"smallProjection\":[true,true,false,true,true]},\"largeTable\":{\"base\":true,\"columnsToRead\":[\"p_partkey\",\"p_type\"],\"filter\":\"{\\\"schemaName\\\":\\\"tpch\\\",\\\"tableName\\\":\\\"part\\\",\\\"columnFilters\\\":{1:{\\\"columnName\\\":\\\"p_type\\\",\\\"columnType\\\":\\\"VARCHAR\\\",\\\"filterJson\\\":\\\"{\\\\\\\"javaType\\\\\\\":\\\\\\\"java.lang.String\\\\\\\",\\\\\\\"isAll\\\\\\\":false,\\\\\\\"isNone\\\\\\\":false,\\\\\\\"allowNull\\\\\\\":false,\\\\\\\"onlyNull\\\\\\\":false,\\\\\\\"ranges\\\\\\\":[],\\\\\\\"discreteValues\\\\\\\":[{\\\\\\\"type\\\\\\\":\\\\\\\"INCLUDED\\\\\\\",\\\\\\\"value\\\\\\\":\\\\\\\"ECONOMY ANODIZED STEEL\\\\\\\"}]}\\\"}}}\",\"inputSplits\":[{\"inputInfos\":[{\"path\":\"minio://pixels-tpch/1g/part/v-0-order/20230513184508_16.pxl\",\"rgLength\":1,\"rgStart\":0}]}],\"keyColumnIds\":[0],\"storageInfo\":{\"$ref\":\"$.chainTables[0].storageInfo\"},\"tableName\":\"part\"},\"output\":{\"encoding\":true,\"fileNames\":[\"0/join\"],\"path\":\"/pixels-lambda-test/6/join_315f30d5a3cb47e6895d617b66b31ad1/part_join_lineitem_join_supplier_join_nation_join_orders_join_customer_join_nation_join_region/\",\"randomFileName\":false,\"storageInfo\":{\"accessKey\":\"output-ak-dummy\",\"endpoint\":\"output-endpoint-dummy\",\"scheme\":\"minio\",\"secretKey\":\"output-sk-dummy\"}},\"partialAggregationPresent\":false,\"postChainJoinsPresent\":false,\"queryId\":6}";
        return JSON.parseObject(json, BroadcastChainJoinInput.class);
    }

    public static BroadcastJoinInput genBroadcastJoinInput(StorageInfo storageInfo)
    {
        String leftFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{2:{\"columnName\":\"p_size\",\"columnType\":\"INT\",\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false,\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"onlyNull\\\":false,\\\"ranges\\\":[],\\\"discreteValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":49},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":14},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":23},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":45},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":19},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":3},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":36},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":9}]}\"}}}";

        // leftFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"part\",\"columnFilters\":{}}";
        String rightFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";

        BroadcastJoinInput joinInput = new BroadcastJoinInput();
        joinInput.setTransId(123456);

        BroadcastTableInfo leftTable = new BroadcastTableInfo();
        leftTable.setColumnsToRead(new String[]{"p_partkey", "p_name", "p_size"});
        leftTable.setKeyColumnIds(new int[]{0});
        leftTable.setTableName("part");
        leftTable.setBase(true);
        leftTable.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20230416155202_0_compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20230416155202_0_compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20230416155202_0_compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20230416155202_0_compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20230416155202_0_compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20230416155202_0_compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20230416155202_0_compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/part/v-0-compact/20230416155202_0_compact.pxl", 28, 4)))));
        leftTable.setFilter(leftFilter);
        leftTable.setStorageInfo(storageInfo);
        joinInput.setSmallTable(leftTable);

        BroadcastTableInfo rightTable = new BroadcastTableInfo();
        rightTable.setColumnsToRead(new String[]{"l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        rightTable.setKeyColumnIds(new int[]{1});
        rightTable.setTableName("lineitem");
        rightTable.setBase(true);
        rightTable.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 28, 4)))));
        rightTable.setFilter(rightFilter);
        rightTable.setStorageInfo(storageInfo);
        joinInput.setLargeTable(rightTable);

        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setSmallColumnAlias(new String[]{"p_name", "p_size"});
        joinInfo.setLargeColumnAlias(new String[]{"l_orderkey", "l_extendedprice", "l_discount"});
        joinInfo.setSmallProjection(new boolean[]{false, true, true});
        joinInfo.setLargeProjection(new boolean[]{true, false, true, true});
        joinInfo.setPostPartition(true);
        joinInfo.setPostPartitionInfo(new PartitionInfo(new int[]{2}, 100));
        joinInput.setJoinInfo(joinInfo);
        joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/unit_tests/",
                storageInfo, true,
                Arrays.asList("broadcast_join_lineitem_part_0")));
        return joinInput;
    }

    public static ScanInput genScanInput(StorageInfo storageInfo, int i)
    {
        String filter =
                "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
        ScanInput scanInput = new ScanInput();
        scanInput.setTransId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("orders");
        tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        tableInfo.setFilter(filter);
        tableInfo.setBase(true);
        tableInfo.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/1g/orders/v-0-order/20230602035632_13.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/1g/orders/v-0-order/20230602035634_14.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/1g/orders/v-0-order/20230602035637_15.pxl", 0, 1)))
        ));

//        tableInfo.setInputSplits(Arrays.asList(
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 0, 4))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 4, 4))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 8, 4))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 12, 4))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 16, 4))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 20, 4))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 24, 4))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 28, 4)))));
        tableInfo.setStorageInfo(storageInfo);
        scanInput.setTableInfo(tableInfo);
        scanInput.setScanProjection(new boolean[]{true, true, true, true});
        scanInput.setPartialAggregationPresent(true);
        PartialAggregationInfo aggregationInfo = new PartialAggregationInfo();
        aggregationInfo.setGroupKeyColumnAlias(new String[]{"o_orderstatus_2", "o_orderdate_3"});
        aggregationInfo.setGroupKeyColumnIds(new int[]{2, 3});
        aggregationInfo.setAggregateColumnIds(new int[]{0});
        aggregationInfo.setResultColumnAlias(new String[]{"sum_o_orderkey_0"});
        aggregationInfo.setResultColumnTypes(new String[]{"bigint"});
        aggregationInfo.setFunctionTypes(new FunctionType[]{FunctionType.SUM});
        scanInput.setPartialAggregationInfo(aggregationInfo);
        scanInput.setOutput(new OutputInfo("pixels-lambda-test/unit_tests/orders_partial_aggr_" + i, false,
                storageInfo, true));
        return scanInput;
    }

    public static PartitionedChainJoinInput genPartitionedChainJoinInput(StorageInfo storageInfo)
    {
        String regionFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"region\",\"columnFilters\":{}}";
        String nationFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"nation\",\"columnFilters\":{}}";
        String supplierFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"supplier\",\"columnFilters\":{}}";

        PartitionedChainJoinInput joinInput = new PartitionedChainJoinInput();
        joinInput.setTransId(123456);

        List<BroadcastTableInfo> chainTables = new ArrayList<>();
        List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();

        BroadcastTableInfo region = new BroadcastTableInfo();
        region.setColumnsToRead(new String[]{"r_regionkey", "r_name"});
        region.setKeyColumnIds(new int[]{0});
        region.setTableName("region");
        region.setBase(true);
        region.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/region/v-0-order/20230416153117_0.pxl", 0, 4)))));
        region.setFilter(regionFilter);
        region.setStorageInfo(storageInfo);
        chainTables.add(region);

        BroadcastTableInfo nation = new BroadcastTableInfo();
        nation.setColumnsToRead(new String[]{"n_nationkey", "n_name", "n_regionkey"});
        nation.setKeyColumnIds(new int[]{2});
        nation.setTableName("nation");
        nation.setBase(true);
        nation.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/nation/v-0-order/20230416135645_0.pxl", 0, 4)))));
        nation.setFilter(nationFilter);
        nation.setStorageInfo(storageInfo);
        chainTables.add(nation);

        ChainJoinInfo chainJoinInfo0 = new ChainJoinInfo();
        chainJoinInfo0.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo0.setSmallProjection(new boolean[]{false, true});
        chainJoinInfo0.setLargeProjection(new boolean[]{true, true, false});
        chainJoinInfo0.setPostPartition(false);
        chainJoinInfo0.setSmallColumnAlias(new String[]{"r_name"});
        chainJoinInfo0.setLargeColumnAlias(new String[]{"n_nationkey", "n_name"});
        chainJoinInfo0.setKeyColumnIds(new int[]{1});
        chainJoinInfos.add(chainJoinInfo0);

        BroadcastTableInfo supplier = new BroadcastTableInfo();
        supplier.setColumnsToRead(new String[]{"s_suppkey", "s_name", "s_nationkey"});
        supplier.setKeyColumnIds(new int[]{2});
        supplier.setTableName("supplier");
        supplier.setBase(true);
        supplier.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/supplier/v-0-compact/20230416155327_0_compact.pxl", 0, 4)))));
        supplier.setFilter(supplierFilter);
        supplier.setStorageInfo(storageInfo);
        chainTables.add(supplier);

        ChainJoinInfo chainJoinInfo1 = new ChainJoinInfo();
        chainJoinInfo1.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo1.setSmallProjection(new boolean[]{true, false, true});
        chainJoinInfo1.setLargeProjection(new boolean[]{true, true, false});
        chainJoinInfo1.setPostPartition(false);
        chainJoinInfo1.setSmallColumnAlias(new String[]{"r_name", "n_name"});
        chainJoinInfo1.setLargeColumnAlias(new String[]{"s_suppkey", "s_name"});
        chainJoinInfo1.setKeyColumnIds(new int[]{2});
        chainJoinInfos.add(chainJoinInfo1);

        Set<Integer> hashValues = new HashSet<>(40);
        for (int i = 0; i < 40; ++i)
        {
            hashValues.add(i);
        }

        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("orders");
        leftTableInfo.setColumnsToRead(new String[]
                {"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
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
        leftTableInfo.setStorageInfo(storageInfo);
        joinInput.setSmallTable(leftTableInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(new String[]
                {"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        rightTableInfo.setKeyColumnIds(new int[]{0});
        rightTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/unit_tests/lineitem_part_0",
                "pixels-lambda-test/unit_tests/lineitem_part_1"));
        rightTableInfo.setParallelism(2);
        rightTableInfo.setBase(false);
        rightTableInfo.setStorageInfo(storageInfo);
        joinInput.setLargeTable(rightTableInfo);

        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setNumPartition(40);
        joinInfo.setHashValues(Arrays.asList(16));
        joinInfo.setSmallColumnAlias(new String[]{"o_custkey", "o_orderstatus", "o_orderdate"});
        joinInfo.setLargeColumnAlias(new String[]{"l_suppkey", "l_extendedprice", "l_discount"});
        joinInfo.setSmallProjection(new boolean[]{false, true, true, true});
        joinInfo.setLargeProjection(new boolean[]{false, true, true, true});
        joinInfo.setPostPartition(false);
        joinInput.setJoinInfo(joinInfo);

        ChainJoinInfo chainJoinInfo2 = new ChainJoinInfo();
        chainJoinInfo2.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo2.setSmallProjection(new boolean[]{true, true, false, true});
        chainJoinInfo2.setLargeProjection(new boolean[]{true, true, true, false, true, true});
        chainJoinInfo2.setPostPartition(true);
        chainJoinInfo2.setPostPartitionInfo(new PartitionInfo(new int[]{3}, 20));
        chainJoinInfo2.setSmallColumnAlias(new String[]{"r_name", "n_name", "s_name"});
        chainJoinInfo2.setLargeColumnAlias(new String[]{
                "o_custkey", "o_orderstatus", "o_orderdate", "l_extendedprice", "l_discount"});
        chainJoinInfo2.setKeyColumnIds(new int[]{3});
        chainJoinInfos.add(chainJoinInfo2);

        joinInput.setChainTables(chainTables);
        joinInput.setChainJoinInfos(chainJoinInfos);

        joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/unit_tests/",
                storageInfo,
                true, Arrays.asList("partitioned_chain_join_0")));
        return joinInput;
    }

    public static PartitionedJoinInput genPartitionedJoinInput(StorageInfo storageInfo)
    {
        Set<Integer> hashValues = new HashSet<>(40);
        for (int i = 0; i < 40; ++i)
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
        leftTableInfo.setStorageInfo(storageInfo);
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
        rightTableInfo.setStorageInfo(storageInfo);
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
        joinInfo.setPostPartitionInfo(new PartitionInfo(new int[]{0}, 100));
        joinInput.setJoinInfo(joinInfo);

        joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/unit_tests/",
                storageInfo,
                true, Arrays.asList("partitioned_join_lineitem_orders_0"))); // force one file currently
        return joinInput;
    }

    public static PartitionInput genPartitionInputOrder(StorageInfo storageInfo, int i)
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
        tableInfo.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_" + i + "_compact.pxl", 28, 4)))));
        tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        tableInfo.setFilter(filter);
        tableInfo.setBase(true);
        tableInfo.setStorageInfo(storageInfo);
        input.setTableInfo(tableInfo);
        input.setProjection(new boolean[]{true, true, true, true});
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setNumPartition(40);
        partitionInfo.setKeyColumnIds(new int[]{0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new OutputInfo("pixels-lambda-test/unit_tests/orders_part_" + i, false,
                storageInfo, true));
        return input;
    }

    public static PartitionInput genPartitionInputLineitem(StorageInfo storageInfo, int i)
    {
        String filter =
                "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
        PartitionInput input = new PartitionInput();
        input.setTransId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("lineitem");
        tableInfo.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_" + i + "_compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_" + i + "_compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_" + i + "_compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_" + i + "_compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_" + i + "_compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_" + i + "_compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_" + i + "_compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_" + i + "_compact.pxl", 28, 4)))));
        tableInfo.setFilter(filter);
        tableInfo.setBase(true);
        tableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        tableInfo.setStorageInfo(storageInfo);
        input.setTableInfo(tableInfo);
        input.setProjection(new boolean[]{true, true, true, true});
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setNumPartition(40);
        partitionInfo.setKeyColumnIds(new int[]{0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new OutputInfo("pixels-lambda-test/unit_tests/lineitem_part_" + i, false,
                storageInfo, true));
        return input;
    }

    public static BiFunction<StorageInfo, Integer, PartitionInput> genPartitionInput(String param)
    {
        switch (param)
        {
            case "order":
                return Utils::genPartitionInputOrder;
            case "lineitem":
                return Utils::genPartitionInputLineitem;
            default:
                return null;
        }
    }
}
