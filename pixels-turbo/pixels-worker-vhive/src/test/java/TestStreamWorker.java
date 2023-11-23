import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.*;
import io.pixelsdb.pixels.worker.vhive.*;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestStreamWorker {

    static StorageInfo minioEndpoint = new StorageInfo(Storage.Scheme.minio,
            ConfigFactory.Instance().getProperty("minio.region"),
            ConfigFactory.Instance().getProperty("minio.endpoint"),
            ConfigFactory.Instance().getProperty("minio.access.key"),
            ConfigFactory.Instance().getProperty("minio.secret.key"));
    static final int numWorkers = 2;
    static StorageInfo httpStorageInfo = new StorageInfo(Storage.Scheme.mock, "http", null, null, null);
//    static final List<List<InputSplit>> genScanInputSplitsList  = Arrays.asList(
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180829_106.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180831_107.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180833_108.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180835_109.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180837_110.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180839_111.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180841_112.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180844_113.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180846_114.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180848_115.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180850_116.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180852_117.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180854_118.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180856_119.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180858_120.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180900_121.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180902_122.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180904_123.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180906_124.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180908_125.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180910_126.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180912_127.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180914_128.pxl", 0, 1)))),
//            Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231012180916_129.pxl", 0, 1))))
//            // n.b. under the ordered layout of Pixels, every .pxl file consists of only 1 row group.
//            //  However, once we adopt the compact layout, this is not the case. Will have to make sure
//            //  we actually read the files in full (currently we only read 1 rowGroup from each file)
//    );

    public static ScanInput genScanInput(StorageInfo inputStorageInfo, StorageInfo outputStorageInfo, int workerId)
    {
        String filter =
                "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
        ScanInput scanInput = new ScanInput();
        scanInput.setTransId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("orders");
        tableInfo.setColumnsToRead(new String[]{"o_custkey", "o_orderstatus", "o_orderdate", "o_orderkey"});
        tableInfo.setFilter(filter);
        tableInfo.setBase(true);

        tableInfo.setInputSplits(
            new ArrayList<>(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124503_106.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124506_107.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124508_108.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124510_109.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124512_110.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124514_111.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124516_112.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124518_113.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124520_114.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124523_115.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124525_116.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124527_117.pxl", 0, 1)))
            )));
//                genScanInputSplitsList.get(workerId));

        tableInfo.setStorageInfo(inputStorageInfo);
        scanInput.setTableInfo(tableInfo);
        scanInput.setScanProjection(new boolean[]{true, true, true, true});
        scanInput.setPartialAggregationPresent(true);
        PartialAggregationInfo aggregationInfo = new PartialAggregationInfo();
        aggregationInfo.setGroupKeyColumnAlias(new String[]{"o_custkey_0", "o_orderstatus_1", "o_orderdate_2"});
        aggregationInfo.setGroupKeyColumnIds(new int[]{0, 1, 2});
        aggregationInfo.setAggregateColumnIds(new int[]{3});
        aggregationInfo.setResultColumnAlias(new String[]{"sum_o_orderkey_3"});
        aggregationInfo.setResultColumnTypes(new String[]{"bigint"});
        aggregationInfo.setFunctionTypes(new FunctionType[]{FunctionType.SUM});
        scanInput.setPartialAggregationInfo(aggregationInfo);
        scanInput.setOutput(new OutputInfo("pixels-lambda-test/unit_tests_intmd/v-0-ordered/orders_partial_aggr_" + workerId, outputStorageInfo, true));
        return scanInput;
    }

    public static ScanInput genScanInputWorker2(StorageInfo inputStorageInfo, StorageInfo outputStorageInfo, int i)
    {
        ScanInput scanInput = genScanInput(inputStorageInfo, outputStorageInfo, i);

        String filter =
                "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("orders");
        tableInfo.setColumnsToRead(new String[]{"o_custkey", "o_orderstatus", "o_orderdate", "o_orderkey"});
        tableInfo.setFilter(filter);
        tableInfo.setBase(true);

        tableInfo.setInputSplits(Arrays.asList(
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124506_107.pxl", 0, 1)))
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124529_118.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124531_119.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124533_120.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124535_121.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124537_122.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124539_123.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124542_124.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124544_125.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124546_126.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124548_127.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124550_128.pxl", 0, 1))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124552_129.pxl", 0, 1)))
        ));

        tableInfo.setStorageInfo(inputStorageInfo);
        scanInput.setTableInfo(tableInfo);

        return scanInput;
    }

    public static AggregationInput genAggregationInput(StorageInfo inputStorageInfo, StorageInfo outputStorageInfo, int i)
    {
        AggregationInput aggregationInput = new AggregationInput();
        aggregationInput.setTransId(123456);
        AggregatedTableInfo tableInfo = new AggregatedTableInfo();
        tableInfo.setTableName("orders");
        tableInfo.setColumnsToRead(new String[]{"o_custkey_0", "o_orderstatus_1", "o_orderdate_2", "sum_o_orderkey_3"});
        tableInfo.setParallelism(1);
        tableInfo.setBase(false);

        List<String> inputFiles = new ArrayList<>();
        for (int k = 0; k < numWorkers; k++) {
            inputFiles.add("pixels-lambda-test/unit_tests_intmd/v-0-ordered/orders_partial_aggr_" + k);
        }
        // print schema
        tableInfo.setInputFiles(inputFiles);

        tableInfo.setStorageInfo(inputStorageInfo);
        aggregationInput.setAggregatedTableInfo(tableInfo);
        AggregationInfo aggregationInfo = new AggregationInfo();
        aggregationInfo.setGroupKeyColumnNames(new String[]{"o_custkey_0", "o_orderstatus_1", "o_orderdate_2"});
        aggregationInfo.setGroupKeyColumnIds(new int[]{0, 1, 2});
        aggregationInfo.setGroupKeyColumnProjection(new boolean[]{true, true, true});
        aggregationInfo.setAggregateColumnIds(new int[]{3});
        aggregationInfo.setResultColumnNames(new String[]{"sum_o_orderkey_3"});
        aggregationInfo.setResultColumnTypes(new String[]{"bigint"});
        aggregationInfo.setFunctionTypes(new FunctionType[]{FunctionType.SUM});
        aggregationInput.setAggregationInfo(aggregationInfo);
        aggregationInput.setOutput(new OutputInfo("pixels-lambda-test/unit_tests_output/v-0-ordered/orders_aggr_" + i, outputStorageInfo, true));
        return aggregationInput;
    }

    @Test
    public void testStreamWorkerSimple() throws ExecutionException, InterruptedException {
//        ScanOutput output = (ScanOutput) InvokerFactory.Instance()
//                .getInvoker(WorkerType.SCAN).invoke(genScanInput(minioEndpoint, minioEndpoint, 0)).get();
//        System.out.println(JSON.toJSONString(output));

        WorkerContext context = new WorkerContext(LogManager.getLogger(TestStreamWorker.class), new WorkerMetrics(), "0");
        ScanWorker worker = new ScanWorker(context);

        ScanOutput output = worker.process(genScanInput(minioEndpoint, minioEndpoint, 0));
        System.out.println(JSON.toJSONString(output));
    }

    public void testStreamWorkerToyPipelined() throws ExecutionException, InterruptedException {

    }

    // port 50051 scan worker -> stream上传送partialAggregate的结果 -> port 50052 aggregate
    //  （scan的结果是不会被其他operator给consume的，所以要传partialAggregate的）
    // join小表是从硬盘直接读到内存里建HashTable。不存在中间结果写出的情况。所以不适合用来做pipeline的demo
    @Test
    public void testStreamWorkerPipelined() throws ExecutionException, InterruptedException {
        // TODO: profiling

        // XXX: In streaming mode, process() only returns after the child operator has started.
        //  This is a deadlock. So we need to run the two workers in two threads for now.

        WorkerContext contextScan = new WorkerContext(LogManager.getLogger(BaseScanStreamWorker.class), new WorkerMetrics(), "0");
//        List<ScanStreamWorker> scanStreamWorkers = new ArrayList<>();
//        for (int i = 0; i < numWorkers; i++) {
//            scanStreamWorkers.add(new ScanStreamWorker(contextScan));
//        }
        ScanStreamWorker scanWorker1 = new ScanStreamWorker(contextScan);
        ScanStreamWorker scanWorker2 = new ScanStreamWorker(contextScan);
        WorkerContext contextAgg = new WorkerContext(LogManager.getLogger(BaseAggregationStreamWorker.class), new WorkerMetrics(), "0");
        AggregationStreamWorker aggregation = new AggregationStreamWorker(contextAgg);
        ExecutorService executor = Executors.newWorkStealingPool();

        // 若需改为不采用pipelining的实现，则将下列所有httpStorageInfo改为minioEndpoint，然后将ScanWorker的基类从BaseScanStreamWorker改为BaseScanWorker，其他几个用到的Worker类同理
        Future<AggregationOutput> aggFuture = executor.submit(() ->
                aggregation.process(genAggregationInput(httpStorageInfo, minioEndpoint, 0))
        );
//        List<Future<ScanOutput>> scanFutures = new ArrayList<>();
//        for (int i = 0; i < numWorkers; i++) {
//            int finalI = i;
//            scanFutures.add(executor.submit(() ->
//                    scanWorkers.get(finalI).process(genScanInput(minioEndpoint, httpStorageInfo, finalI))
//            ));
//        }
        Future<ScanOutput> scanFuture1 = executor.submit(() ->
                scanWorker1.process(genScanInput(minioEndpoint, httpStorageInfo, 0))
        );
        Future<ScanOutput> scanFuture2 = executor.submit(() ->
                scanWorker2.process(genScanInputWorker2(minioEndpoint, httpStorageInfo, 1))
        );

        AggregationOutput aggOutput = aggFuture.get();
        System.out.println(JSON.toJSONString(aggOutput));

        executor.shutdown();
    }
    /*
     Can check the result by running the following SQL queries:

         -- Find rows that are in the test output but not in the correct result (result obtained by direcly running the query in Trino CLI)
         SELECT *
         FROM tpch_lambda_test.tpch_lambda_test_result_2 AS result_2
         LEFT JOIN (SELECT o_custkey, o_orderstatus, o_orderdate, SUM(o_orderkey) AS sum_o_orderkey FROM orders GROUP BY o_custkey, o_orderstatus, o_orderdate) AS orders_agg
         ON result_2.o_custkey_0 = orders_agg.o_custkey AND result_2.o_orderstatus_1 = orders_agg.o_orderstatus AND result_2.o_orderdate_2 = orders_agg.o_orderdate AND result_2.sum_o_orderkey_3 = orders_agg.sum_o_orderkey
         WHERE orders_agg.o_custkey IS NULL
         ORDER BY o_custkey_0, o_orderdate_2;

         -- Find rows that are not in the test output but are in the correct result
         SELECT *
         FROM tpch_lambda_test.tpch_lambda_test_result_2 AS result_2
         RIGHT JOIN (SELECT o_custkey, o_orderstatus, o_orderdate, SUM(o_orderkey) AS sum_o_orderkey FROM orders GROUP BY o_custkey, o_orderstatus, o_orderdate) AS orders_agg
         ON result_2.o_custkey_0 = orders_agg.o_custkey AND result_2.o_orderstatus_1 = orders_agg.o_orderstatus AND result_2.o_orderdate_2 = orders_agg.o_orderdate AND result_2.sum_o_orderkey_3 = orders_agg.sum_o_orderkey
         WHERE result_2.o_custkey_0 IS NULL
         ORDER BY o_custkey_0, o_orderdate_2;

     Or alternatively: run the control group experiment (non-pipelined) and compare the results.

         CREATE TABLE IF NOT EXISTS tpch_lambda_test.tpch_lambda_test_result_2 (
             o_custkey_0 bigint,
             o_orderstatus_1 char(1),
             o_orderdate_2 date,
             sum_o_orderkey_3 bigint
         ) WITH (storage='minio', paths='minio://pixels-lambda-test/unit_tests_output/');

         SELECT *
         FROM tpch_lambda_test.tpch_lambda_test_result AS result
         LEFT JOIN tpch_lambda_test.tpch_lambda_test_result_2 AS result_2
         ON result.o_custkey_0 = result_2.o_custkey_0 AND result.o_orderstatus_1 = result_2.o_orderstatus_1 AND result.o_orderdate_2 = result_2.o_orderdate_2 AND result.sum_o_orderkey_3 = result_2.sum_o_orderkey_3
         WHERE result_2.o_custkey_0 IS NULL
         ORDER BY result.o_custkey_0, result.o_orderdate_2;

         SELECT *
         FROM tpch_lambda_test.tpch_lambda_test_result AS result
         RIGHT JOIN tpch_lambda_test.tpch_lambda_test_result_2 AS result_2
         ON result.o_custkey_0 = result_2.o_custkey_0 AND result.o_orderstatus_1 = result_2.o_orderstatus_1 AND result.o_orderdate_2 = result_2.o_orderdate_2 AND result.sum_o_orderkey_3 = result_2.sum_o_orderkey_3
         WHERE result.o_custkey_0 IS NULL
         ORDER BY result.o_custkey_0, result.o_orderdate_2;

     Both queries should return empty result.
     */

    // 对照组实验
    // 跑之前删一下minio上面的中间结果
    @Test
    public void testWorkerNonPipelined() throws ExecutionException, InterruptedException {
        WorkerContext contextScan = new WorkerContext(LogManager.getLogger(BaseScanWorker.class), new WorkerMetrics(), "0");
        ScanWorker scanWorker1 = new ScanWorker(contextScan);
        ScanWorker scanWorker2 = new ScanWorker(contextScan);
        WorkerContext contextAgg = new WorkerContext(LogManager.getLogger(BaseAggregationWorker.class), new WorkerMetrics(), "0");
        AggregationWorker aggregation = new AggregationWorker(contextAgg);
        ExecutorService executor = Executors.newWorkStealingPool();

        AggregationInput aggInput = genAggregationInput(minioEndpoint, minioEndpoint, 0);
        aggInput.setOutput(new OutputInfo("pixels-lambda-test/unit_tests_output_unstreamed/v-0-ordered/orders_aggr_0", minioEndpoint, true));
        Future<AggregationOutput> aggFuture = executor.submit(() ->
                aggregation.process(aggInput)
        );
        Future<ScanOutput> scanFuture1 = executor.submit(() ->
                scanWorker1.process(genScanInput(minioEndpoint, minioEndpoint, 0))
        );
        Future<ScanOutput> scanFuture2 = executor.submit(() ->
                scanWorker2.process(genScanInputWorker2(minioEndpoint, minioEndpoint, 1))
        );

        AggregationOutput aggOutput = aggFuture.get();
        System.out.println(JSON.toJSONString(aggOutput));

        executor.shutdown();
    }

    static final int test2NumHashes = 3;
    static final List<List<InputSplit>> test2ScanInputSplitsArray = Arrays.asList(
            Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163802_1.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163805_2.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163807_3.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163809_4.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163812_5.pxl", 0, 1)))
            ),
            Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163814_6.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163816_7.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163818_8.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163820_9.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163822_10.pxl", 0, 1)))
            ),
            Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163824_11.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163826_12.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163829_13.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163831_14.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163833_15.pxl", 0, 1)))
            ),
            Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163835_16.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163837_17.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163839_18.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163841_19.pxl", 0, 1)))
            ),
            Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163843_20.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163845_21.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163847_22.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163849_23.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231119163851_24.pxl", 0, 1)))
            ),
            Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/customer/v-0-ordered/20231119111116_0.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/customer/v-0-ordered/20231119111118_1.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/customer/v-0-ordered/20231119111120_2.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/customer/v-0-ordered/20231119111121_3.pxl", 0, 1))),
                    new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/customer/v-0-ordered/20231119111123_4.pxl", 0, 1)))
            )
    );
    static final int test2NumScanWorkers = test2ScanInputSplitsArray.size();
    static final String test2IntermediateOutputPathLargeTablePrefix = "pixels-lambda-test/unit_tests_intmd_order_parted/v-0-ordered/orders_parted";
    static final String test2IntermediateOutputPathSmallTablePrefix = "pixels-lambda-test/unit_tests_intmd_customer_parted/v-0-ordered/customer_parted";

    public static PartitionInput test2GenParInput(StorageInfo inputStorageInfo, StorageInfo outputStorageInfo, int workerId)
    {
        if (0 <= workerId && workerId <= test2NumScanWorkers - 2)
        {
            PartitionInput parInput = new PartitionInput();
            String filter =
                    "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";

            ScanTableInfo tableInfo = new ScanTableInfo();
            tableInfo.setTableName("orders");
            tableInfo.setColumnsToRead(new String[]{"o_custkey", "o_orderstatus", "o_orderdate", "o_orderkey"});
            tableInfo.setFilter(filter);
            tableInfo.setBase(true);
            tableInfo.setInputSplits(test2ScanInputSplitsArray.get(workerId));
            tableInfo.setStorageInfo(inputStorageInfo);

            PartitionInfo parInfo = new PartitionInfo();
            parInfo.setKeyColumnIds(new int[]{0});
            parInfo.setNumPartition(test2NumHashes);

            parInput.setOperatorName(workerId + "/" + (test2NumScanWorkers - 1));  // pass the workerId into the worker
            parInput.setTransId(123456);
            parInput.setTableInfo(tableInfo);
            parInput.setProjection(new boolean[]{true, true, true, true});
            parInput.setPartitionInfo(parInfo);
            parInput.setOutput(new OutputInfo(test2IntermediateOutputPathLargeTablePrefix, outputStorageInfo, true));
            return parInput;
        }
        else if (workerId == test2NumScanWorkers - 1) {
            PartitionInput parInput = new PartitionInput();
            String filter =
                    "{\"schemaName\":\"tpch\",\"tableName\":\"customer\",\"columnFilters\":{}}";

            ScanTableInfo tableInfo = new ScanTableInfo();
            tableInfo.setTableName("customer");
            tableInfo.setColumnsToRead(new String[]{"c_custkey", "c_name", "c_address", "c_phone"});
            tableInfo.setFilter(filter);
            tableInfo.setBase(true);
            tableInfo.setInputSplits(test2ScanInputSplitsArray.get(workerId));
            tableInfo.setStorageInfo(inputStorageInfo);

            PartitionInfo parInfo = new PartitionInfo();
            parInfo.setKeyColumnIds(new int[]{0});
            parInfo.setNumPartition(test2NumHashes);

            parInput.setOperatorName("0/1");  // pass the workerId into the worker
            parInput.setTransId(123456);
            parInput.setTableInfo(tableInfo);
            parInput.setProjection(new boolean[]{true, true, true, true});
            parInput.setPartitionInfo(parInfo);
            parInput.setOutput(new OutputInfo(test2IntermediateOutputPathSmallTablePrefix, outputStorageInfo, true));  // todo
            return parInput;
        }
        else {
            throw new IllegalArgumentException("workerId must be in [0, " + test2NumScanWorkers + ")");
        }
    }

    public static PartitionInput test2GenParInputNonPipelined(StorageInfo inputStorageInfo, StorageInfo outputStorageInfo, int workerId)
    {
        PartitionInput parInput = test2GenParInput(inputStorageInfo, outputStorageInfo, workerId);
        if (0 <= workerId && workerId <= test2NumScanWorkers - 2) {
            parInput.setOutput(new OutputInfo(test2IntermediateOutputPathLargeTablePrefix + "/" + (char) ('A' + workerId), outputStorageInfo, true));
        }
        else if (workerId == test2NumScanWorkers - 1) {
            parInput.setOutput(new OutputInfo(test2IntermediateOutputPathSmallTablePrefix + "/" + (char) ('A'), outputStorageInfo, true));
        }
        else {
            throw new IllegalArgumentException("workerId must be in [0, " + test2NumScanWorkers + ")");
        }
        return parInput;
    }

    static final List<String> test2ParJoinInputFilesLargeTable = Arrays.asList(
            test2IntermediateOutputPathLargeTablePrefix + "/0",
            test2IntermediateOutputPathLargeTablePrefix + "/1",
            test2IntermediateOutputPathLargeTablePrefix + "/2"
    );  // Must correspond to BasePartitionStreamWorker.java:157
    static final List<String> test2ParJoinInputFilesSmallTable = Arrays.asList(
            test2IntermediateOutputPathSmallTablePrefix + "/0",
            test2IntermediateOutputPathSmallTablePrefix + "/1",
            test2IntermediateOutputPathSmallTablePrefix + "/2"
    );
    static final List<String> test2ParJoinInputFilesLargeTableNonPipelined = Arrays.asList(
            test2IntermediateOutputPathLargeTablePrefix + "/A",
            test2IntermediateOutputPathLargeTablePrefix + "/B",
            test2IntermediateOutputPathLargeTablePrefix + "/C",
            test2IntermediateOutputPathLargeTablePrefix + "/D",
            test2IntermediateOutputPathLargeTablePrefix + "/E"
    );  // Must correspond to BasePartitionStreamWorker.java:157
    static final List<String> test2ParJoinInputFilesSmallTableNonPipelined = Arrays.asList(
            test2IntermediateOutputPathSmallTablePrefix + "/A"
    );

    public static PartitionedJoinInput test2GenParJoinInput(StorageInfo inputStorageInfo, StorageInfo outputStorageInfo, int workerId)
    {
        assert(test2ParJoinInputFilesLargeTable.size() == test2NumHashes);
        assert(test2ParJoinInputFilesSmallTable.size() == test2NumHashes);

        PartitionedJoinInput parJoinInput = new PartitionedJoinInput();

        PartitionedTableInfo largeTable = new PartitionedTableInfo();
        largeTable.setTableName("orders");
        largeTable.setColumnsToRead(new String[]{"o_custkey", "o_orderstatus", "o_orderdate", "o_orderkey"});
        largeTable.setBase(false);  // ???
        largeTable.setStorageInfo(inputStorageInfo);
        largeTable.setInputFiles(Collections.singletonList(test2ParJoinInputFilesLargeTable.get(workerId)));
        largeTable.setParallelism(2);  // ???
        largeTable.setKeyColumnIds(new int[]{0});

        PartitionedTableInfo smallTable = new PartitionedTableInfo();
        smallTable.setTableName("customer");
        smallTable.setColumnsToRead(new String[]{"c_custkey", "c_name", "c_address", "c_phone"});
        smallTable.setBase(false);  // ???
        smallTable.setStorageInfo(inputStorageInfo);
        smallTable.setInputFiles(Collections.singletonList(test2ParJoinInputFilesSmallTable.get(workerId)));
        smallTable.setParallelism(2);
        smallTable.setKeyColumnIds(new int[]{0});

        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
        joinInfo.setNumPartition(test2NumHashes);
        joinInfo.setHashValues(Collections.singletonList(workerId));
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setLargeProjection(new boolean[]{true, true, true, true});
        joinInfo.setLargeColumnAlias(new String[]{"o_custkey", "o_orderstatus", "o_orderdate", "o_orderkey"});
        joinInfo.setSmallProjection(new boolean[]{true, true, true, true});
        joinInfo.setSmallColumnAlias(new String[]{"c_custkey", "c_name", "c_address", "c_phone"});

        MultiOutputInfo output = new MultiOutputInfo(
                "pixels-lambda-test/unit_tests_parjoin/v-0-ordered/",
                outputStorageInfo, true, Arrays.asList("orders_join_customer")
                );

        parJoinInput.setPartialAggregationPresent(false);
        parJoinInput.setOutput(output);
        parJoinInput.setSmallTable(smallTable);
        parJoinInput.setLargeTable(largeTable);
        parJoinInput.setJoinInfo(joinInfo);
        parJoinInput.setTransId(123456);
        return parJoinInput;
    }

    public static PartitionedJoinInput test2GenParJoinInputNonPipelined(StorageInfo inputStorageInfo, StorageInfo outputStorageInfo, int workerId)
    {
        PartitionedJoinInput parJoinInput = test2GenParJoinInput(inputStorageInfo, outputStorageInfo, workerId);
        parJoinInput.getLargeTable().setInputFiles(test2ParJoinInputFilesLargeTableNonPipelined);
        parJoinInput.getSmallTable().setInputFiles(test2ParJoinInputFilesSmallTableNonPipelined);
        return parJoinInput;
    }

    @Test
    public void test2NonPipelined() throws ExecutionException, InterruptedException {
        // orders JOIN customer
        // 6 partition workers (5 for orders table + 1 for customer table) fully connected to 3 partitioned join workers

        WorkerContext contextScan = new WorkerContext(LogManager.getLogger(BasePartitionWorker.class), new WorkerMetrics(), "0");
        PartitionWorker partitionWorker1 = new PartitionWorker(contextScan);
        PartitionWorker partitionWorker2 = new PartitionWorker(contextScan);
        PartitionWorker partitionWorker3 = new PartitionWorker(contextScan);
        PartitionWorker partitionWorker4 = new PartitionWorker(contextScan);
        PartitionWorker partitionWorker5 = new PartitionWorker(contextScan);
        PartitionWorker partitionWorker6 = new PartitionWorker(contextScan);

        WorkerContext contextParJoin = new WorkerContext(LogManager.getLogger(BasePartitionedJoinWorker.class), new WorkerMetrics(), "0");
        PartitionedJoinWorker parJoinWorker1 = new PartitionedJoinWorker(contextParJoin);
        PartitionedJoinWorker parJoinWorker2 = new PartitionedJoinWorker(contextParJoin);
        PartitionedJoinWorker parJoinWorker3 = new PartitionedJoinWorker(contextParJoin);

        ExecutorService executor = Executors.newFixedThreadPool(9);
        Future<JoinOutput>[] parJoinFutures = new Future[] {
                executor.submit(() -> parJoinWorker1.process(test2GenParJoinInputNonPipelined(minioEndpoint, minioEndpoint, 0))),
                executor.submit(() -> parJoinWorker2.process(test2GenParJoinInputNonPipelined(minioEndpoint, minioEndpoint, 1))),
                executor.submit(() -> parJoinWorker3.process(test2GenParJoinInputNonPipelined(minioEndpoint, minioEndpoint, 2)))
        };
        Future<PartitionOutput>[] scanFutures = new Future[] {
                executor.submit(() -> partitionWorker1.process(test2GenParInputNonPipelined(minioEndpoint, minioEndpoint, 0))),
                executor.submit(() -> partitionWorker2.process(test2GenParInputNonPipelined(minioEndpoint, minioEndpoint, 1))),
                executor.submit(() -> partitionWorker3.process(test2GenParInputNonPipelined(minioEndpoint, minioEndpoint, 2))),
                executor.submit(() -> partitionWorker4.process(test2GenParInputNonPipelined(minioEndpoint, minioEndpoint, 3))),
                executor.submit(() -> partitionWorker5.process(test2GenParInputNonPipelined(minioEndpoint, minioEndpoint, 4))),
                executor.submit(() -> partitionWorker6.process(test2GenParInputNonPipelined(minioEndpoint, minioEndpoint, 5)))
        };

        for (Future<JoinOutput> future: parJoinFutures) {
            JoinOutput parJoinOutput = future.get();
            System.out.println(JSON.toJSONString(parJoinOutput));
        }

        executor.shutdown();
    }

    @Test
    public void test2Pipelined() throws ExecutionException, InterruptedException {
        // orders JOIN customer
        // 5 partition workers (4 for orders table + 1 for customer table) fully connected to 3 partitioned join workers

        WorkerContext contextScan = new WorkerContext(LogManager.getLogger(BasePartitionStreamWorker.class), new WorkerMetrics(), "0");
        PartitionStreamWorker partitionWorker1 = new PartitionStreamWorker(contextScan);
        PartitionStreamWorker partitionWorker2 = new PartitionStreamWorker(contextScan);
        PartitionStreamWorker partitionWorker3 = new PartitionStreamWorker(contextScan);
        PartitionStreamWorker partitionWorker4 = new PartitionStreamWorker(contextScan);
        PartitionStreamWorker partitionWorker5 = new PartitionStreamWorker(contextScan);
        PartitionStreamWorker partitionWorker6 = new PartitionStreamWorker(contextScan);

        WorkerContext contextParJoin = new WorkerContext(LogManager.getLogger(BasePartitionedJoinStreamWorker.class), new WorkerMetrics(), "0");
        PartitionedJoinStreamWorker parJoinWorker1 = new PartitionedJoinStreamWorker(contextParJoin);
        PartitionedJoinStreamWorker parJoinWorker2 = new PartitionedJoinStreamWorker(contextParJoin);
        PartitionedJoinStreamWorker parJoinWorker3 = new PartitionedJoinStreamWorker(contextParJoin);

        ExecutorService executor = Executors.newFixedThreadPool(9);
        Future<JoinOutput>[] parJoinFutures = new Future[] {
                executor.submit(() -> parJoinWorker1.process(test2GenParJoinInput(httpStorageInfo, minioEndpoint, 0))),
                executor.submit(() -> parJoinWorker2.process(test2GenParJoinInput(httpStorageInfo, minioEndpoint, 1))),
                executor.submit(() -> parJoinWorker3.process(test2GenParJoinInput(httpStorageInfo, minioEndpoint, 2)))
        };
        Future<PartitionOutput>[] scanFutures = new Future[] {
                executor.submit(() -> partitionWorker1.process(test2GenParInput(minioEndpoint, httpStorageInfo, 0))),
                executor.submit(() -> partitionWorker2.process(test2GenParInput(minioEndpoint, httpStorageInfo, 1))),
                executor.submit(() -> partitionWorker3.process(test2GenParInput(minioEndpoint, httpStorageInfo, 2))),
                executor.submit(() -> partitionWorker4.process(test2GenParInput(minioEndpoint, httpStorageInfo, 3))),
                executor.submit(() -> partitionWorker5.process(test2GenParInput(minioEndpoint, httpStorageInfo, 4))),
                executor.submit(() -> partitionWorker6.process(test2GenParInput(minioEndpoint, httpStorageInfo, 5)))
        };

        for (Future<JoinOutput> future: parJoinFutures) {
            JoinOutput parJoinOutput = future.get();
            System.out.println(JSON.toJSONString(parJoinOutput));
        }

        executor.shutdown();
    }
}
