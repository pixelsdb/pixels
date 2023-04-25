import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.*;

import java.util.*;

public class Utils {
    public static AggregationInput genAggregationInput() {
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
        aggregationInput.setGroupKeyColumnNames(new String[]{"o_orderstatus_2", "o_orderdate_3"});
        aggregationInput.setGroupKeyColumnProjection(new boolean[]{true, true});
        aggregationInput.setResultColumnNames(new String[]{"sum_o_orderkey_0"});
        aggregationInput.setResultColumnTypes(new String[]{"bigint"});
        aggregationInput.setFunctionTypes(new FunctionType[]{FunctionType.SUM});
        aggregationInput.setOutput(new OutputInfo("pixels-lambda-test/orders_final_aggr", false,
                new StorageInfo(Storage.Scheme.s3, null, null, null), true));
        return aggregationInput;
    }

    public static BroadcastChainJoinInput genBroadcastChainJoinInput() {
        String regionFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"region\",\"columnFilters\":{}}";
        String nationFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"nation\",\"columnFilters\":{}}";
        String supplierFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"supplier\",\"columnFilters\":{}}";
        String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";

        BroadcastChainJoinInput joinInput = new BroadcastChainJoinInput();
        joinInput.setQueryId(123456);

        List<BroadcastTableInfo> leftTables = new ArrayList<>();
        List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();

        BroadcastTableInfo region = new BroadcastTableInfo();
        region.setColumnsToRead(new String[]{"r_regionkey", "r_name"});
        region.setKeyColumnIds(new int[]{0});
        region.setTableName("region");
        region.setBase(true);
        region.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/region/v-0-order/20220313093112_0.pxl", 0, 4)))));
        region.setFilter(regionFilter);
        leftTables.add(region);

        BroadcastTableInfo nation = new BroadcastTableInfo();
        nation.setColumnsToRead(new String[]{"n_nationkey", "n_name", "n_regionkey"});
        nation.setKeyColumnIds(new int[]{2});
        nation.setTableName("nation");
        nation.setBase(true);
        nation.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/nation/v-0-order/20220313080937_0.pxl", 0, 4)))));
        nation.setFilter(nationFilter);
        leftTables.add(nation);

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
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/supplier/v-0-compact/20220313101902_0.compact.pxl", 0, 4)))));
        supplier.setFilter(supplierFilter);
        leftTables.add(supplier);

        ChainJoinInfo chainJoinInfo1 = new ChainJoinInfo();
        chainJoinInfo1.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo1.setSmallProjection(new boolean[]{true, false, true});
        chainJoinInfo1.setLargeProjection(new boolean[]{true, true, false});
        chainJoinInfo1.setPostPartition(false);
        chainJoinInfo1.setSmallColumnAlias(new String[]{"r_name", "n_name"});
        chainJoinInfo1.setLargeColumnAlias(new String[]{"s_suppkey", "s_name"});
        chainJoinInfo1.setKeyColumnIds(new int[]{2});
        chainJoinInfos.add(chainJoinInfo1);

        joinInput.setChainTables(leftTables);
        joinInput.setChainJoinInfos(chainJoinInfos);

        BroadcastTableInfo lineitem = new BroadcastTableInfo();
        lineitem.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        lineitem.setKeyColumnIds(new int[]{1});
        lineitem.setTableName("lineitem");
        lineitem.setBase(true);
        lineitem.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 28, 4)))));
        lineitem.setFilter(lineitemFilter);
        joinInput.setLargeTable(lineitem);

        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setSmallColumnAlias(new String[]{"r_name", "n_name", "s_name"});
        joinInfo.setLargeColumnAlias(new String[]{"l_orderkey", "l_extendedprice", "l_discount"});
        joinInfo.setSmallProjection(new boolean[]{true, true, false, true});
        joinInfo.setLargeProjection(new boolean[]{true, false, true, true});
        joinInfo.setPostPartition(true);
        joinInfo.setPostPartitionInfo(new PartitionInfo(new int[]{3}, 100));
        joinInput.setJoinInfo(joinInfo);

        joinInput.setOutput(new MultiOutputInfo("pixels-lambda/",
                new StorageInfo(Storage.Scheme.minio, "http://172.31.32.193:9000",
                        "lambda", "password"), true,
                Arrays.asList("chain-join-0", "chain-join-1", "chain-join-2", "chain-join-3",
                        "chain-join-4", "chain-join-5", "chain-join-6", "chain-join-7")));
        return joinInput;
    }

    public static BroadcastJoinInput genBroadcastJoinInput() {
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

        BroadcastTableInfo leftTable = new BroadcastTableInfo();
        leftTable.setColumnsToRead(new String[]{"p_partkey", "p_name", "p_size"});
        leftTable.setKeyColumnIds(new int[]{0});
        leftTable.setTableName("part");
        leftTable.setBase(true);
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
        joinInput.setSmallTable(leftTable);

        BroadcastTableInfo rightTable = new BroadcastTableInfo();
        rightTable.setColumnsToRead(new String[]{"l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        rightTable.setKeyColumnIds(new int[]{1});
        rightTable.setTableName("lineitem");
        rightTable.setBase(true);
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
        joinInput.setLargeTable(rightTable);

        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setSmallColumnAlias(new String[]{"p_name", "p_size"});
        joinInfo.setLargeColumnAlias(new String[]{"l_orderkey", "l_extendedprice", "l_discount"});
        joinInfo.setSmallProjection(new boolean[]{false, true, true});
        joinInfo.setLargeProjection(new boolean[]{true, false, true, true});
        joinInfo.setPostPartition(true);
        joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {2}, 100));
        joinInput.setJoinInfo(joinInfo);
        joinInput.setOutput(new MultiOutputInfo("pixels-lambda/",
                new StorageInfo(Storage.Scheme.minio, "http://172.31.32.193:9000",
                        "lambda", "password"), true,
                Arrays.asList("broadcast-join-0","broadcast-join-1","broadcast-join-2","broadcast-join-3",
                        "broadcast-join-4","broadcast-join-5","broadcast-join-6","broadcast-join-7")));
        return joinInput;
    }

    public static ScanInput genScanInput() {
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
        return scanInput;
    }

    public static PartitionedChainJoinInput genPartitionedChainJoinInput() {
        String regionFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"region\",\"columnFilters\":{}}";
        String nationFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"nation\",\"columnFilters\":{}}";
        String supplierFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"supplier\",\"columnFilters\":{}}";

        PartitionedChainJoinInput joinInput = new PartitionedChainJoinInput();
        joinInput.setQueryId(123456);

        List<BroadcastTableInfo> chainTables = new ArrayList<>();
        List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();

        BroadcastTableInfo region = new BroadcastTableInfo();
        region.setColumnsToRead(new String[]{"r_regionkey", "r_name"});
        region.setKeyColumnIds(new int[]{0});
        region.setTableName("region");
        region.setBase(true);
        region.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/region/v-0-order/20220313093112_0.pxl", 0, 4)))));
        region.setFilter(regionFilter);
        chainTables.add(region);

        BroadcastTableInfo nation = new BroadcastTableInfo();
        nation.setColumnsToRead(new String[]{"n_nationkey", "n_name", "n_regionkey"});
        nation.setKeyColumnIds(new int[]{2});
        nation.setTableName("nation");
        nation.setBase(true);
        nation.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/nation/v-0-order/20220313080937_0.pxl", 0, 4)))));
        nation.setFilter(nationFilter);
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
                new InputSplit(Arrays.asList(new InputInfo(
                        "pixels-tpch/supplier/v-0-compact/20220313101902_0.compact.pxl",
                        0, 4)))));
        supplier.setFilter(supplierFilter);
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
        for (int i = 0 ; i < 40; ++i)
        {
            hashValues.add(i);
        }

        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("orders");
        leftTableInfo.setColumnsToRead(new String[]
                {"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        leftTableInfo.setKeyColumnIds(new int[]{0});
        leftTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/orders_part_0",
                "pixels-lambda-test/orders_part_1",
                "pixels-lambda-test/orders_part_2",
                "pixels-lambda-test/orders_part_3",
                "pixels-lambda-test/orders_part_4",
                "pixels-lambda-test/orders_part_5",
                "pixels-lambda-test/orders_part_6",
                "pixels-lambda-test/orders_part_7"));
        leftTableInfo.setParallelism(8);
        joinInput.setSmallTable(leftTableInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(new String[]
                {"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        rightTableInfo.setKeyColumnIds(new int[]{0});
        rightTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/lineitem_part_0",
                "pixels-lambda-test/lineitem_part_1"));
        rightTableInfo.setParallelism(2);
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
        chainJoinInfo2.setPostPartitionInfo(new PartitionInfo(new int[] {3}, 20));
        chainJoinInfo2.setSmallColumnAlias(new String[]{"r_name", "n_name", "s_name"});
        chainJoinInfo2.setLargeColumnAlias(new String[]{
                "o_custkey", "o_orderstatus", "o_orderdate", "l_extendedprice", "l_discount"});
        chainJoinInfo2.setKeyColumnIds(new int[]{3});
        chainJoinInfos.add(chainJoinInfo2);

        joinInput.setChainTables(chainTables);
        joinInput.setChainJoinInfos(chainJoinInfos);

        joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/",
                new StorageInfo(Storage.Scheme.s3, null, null, null),
                true, Arrays.asList("partitioned-chain-join-0", "partitioned-chain-join-1")));
        return joinInput;
    }

    public static PartitionedJoinInput genPartitionedJoinInput() {
        Set<Integer> hashValues = new HashSet<>(40);
        for (int i = 0 ; i < 40; ++i)
        {
            hashValues.add(i);
        }

        PartitionedJoinInput joinInput = new PartitionedJoinInput();
        joinInput.setQueryId(123456);
        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("orders");
        leftTableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        leftTableInfo.setKeyColumnIds(new int[]{0});
        leftTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/orders_part_0",
                "pixels-lambda-test/orders_part_1",
                "pixels-lambda-test/orders_part_2",
                "pixels-lambda-test/orders_part_3",
                "pixels-lambda-test/orders_part_4",
                "pixels-lambda-test/orders_part_5",
                "pixels-lambda-test/orders_part_6",
                "pixels-lambda-test/orders_part_7"));
        leftTableInfo.setParallelism(8);
        joinInput.setSmallTable(leftTableInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        rightTableInfo.setKeyColumnIds(new int[]{0});
        rightTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/lineitem_part_0",
                "pixels-lambda-test/lineitem_part_1"));
        rightTableInfo.setParallelism(2);
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

        joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/",
                new StorageInfo(Storage.Scheme.s3, null, null, null),
                true, Arrays.asList("partitioned-join-0", "partitioned-join-1")));
        return joinInput;
    }

    public static PartitionInput genPartitionInputOrder() {
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
        input.setQueryId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("orders");
        tableInfo.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_0_compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_0_compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_0_compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_0_compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_0_compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_0_compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_0_compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-compact/20230416154127_0_compact.pxl", 28, 4)))));
        tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        tableInfo.setFilter(filter);
        input.setTableInfo(tableInfo);
        input.setProjection(new boolean[] {true, true, true, true});
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setNumPartition(40);
        partitionInfo.setKeyColumnIds(new int[]{0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new OutputInfo("pixels-lambda-test/orders_part_6", false,
                new StorageInfo(Storage.Scheme.s3, null, null, null), true));
        return input;
    }

    public static PartitionInput genPartitionInputLineitem() {
        String filter =
                "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
        PartitionInput input = new PartitionInput();
        input.setQueryId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("lineitem");
        tableInfo.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20220313102020_0.compact.pxl", 28, 4)))));
        tableInfo.setFilter(filter);
        tableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        input.setTableInfo(tableInfo);
        input.setProjection(new boolean[] {true, true, true, true});
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setNumPartition(40);
        partitionInfo.setKeyColumnIds(new int[]{0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new OutputInfo("pixels-lambda-test/lineitem_part_0", false,
                new StorageInfo(Storage.Scheme.s3, null, null, null),true));
        return input;
    }

    public static PartitionInput genPartitionInput(String param) {
        switch (param) {
            case "order":
                return genPartitionInputOrder();
            case "lineitem":
                return genPartitionInputLineitem();
            default:
                return null;
        }
    }
}
