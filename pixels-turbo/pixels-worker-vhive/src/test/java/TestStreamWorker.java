import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.*;
import io.pixelsdb.pixels.worker.vhive.*;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124503_106.pxl", 0, 1)))
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124506_107.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124508_108.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124510_109.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124512_110.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124514_111.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124516_112.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124518_113.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124520_114.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124523_115.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124525_116.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124527_117.pxl", 0, 1)))
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
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124506_107.pxl", 0, 1)))
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124529_118.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124531_119.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124533_120.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124535_121.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124537_122.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124539_123.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124542_124.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124544_125.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124546_126.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124548_127.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124550_128.pxl", 0, 1))),
//                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/orders/v-0-ordered/20231024124552_129.pxl", 0, 1)))
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
}
