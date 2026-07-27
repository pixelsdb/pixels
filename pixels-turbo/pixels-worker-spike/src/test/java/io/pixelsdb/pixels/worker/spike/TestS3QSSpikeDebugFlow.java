/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.worker.spike;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.SpikeWorkerRequest;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinatorFactory;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateServer;
import io.pixelsdb.pixels.planner.plan.physical.PartitionedJoinS3QSOperator;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.spike.handler.SpikeWorker;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Issue 1277 中用于排查 S3QS 与 Spike worker 分发链路的真实代码调试入口。
 *
 * 该测试默认通过 @Ignore 关闭，因为它不会使用 mock 或桩对象替代执行路径，
 * 而是会真正进入 worker 的生产执行链路：
 *
 *   RequestHandlerImpl
 *     -> WorkerService
 *     -> S3QSPartitionWorker
 *     -> S3QSStageWorkerRunner
 *     -> WorkerCoordinateService / WorkerCoordinateServer
 *     -> StageCoordinator task queue
 *     -> BasePartitionWorker
 *     -> WorkerCommon / S3QS / S3 / SQS
 *
 * 因此它不是一个用于解释逻辑的轻量级单元测试，而是一个便于在 IDE 中下断点、
 * 观察真实调用栈和运行时对象状态的集成调试入口。启用该测试前，需要准备真实的
 * Pixels 输入文件、AWS S3/SQS 资源以及本地配置，否则 worker 会在读取输入、
 * 初始化 shuffle 存储或访问队列时失败。
 */
public class TestS3QSSpikeDebugFlow
{
    @Ignore("仅用于调试的集成入口。需要真实 Pixels 输入数据、AWS S3/SQS 资源和运行配置。")
    @Test
    public void realSpikeRequestRunsS3QSPartitionStageWorkerThroughCoordinator() throws Exception
    {
        /*
         * 必填环境变量：
         *
         * PIXELS_S3QS_DEBUG_INPUT_PATH
         *   真实存在的 Pixels 文件路径，且必须能被当前配置的输入存储后端读取。
         *   示例：s3://bucket/path/source.pxl
         *
         * PIXELS_S3QS_DEBUG_OBJECT_PREFIX
         *   S3QS 写入 DATA 类型 S3QueueMessage 时使用的对象前缀。
         *   该前缀会作为 shuffle 中间数据的写入位置，通常应指向一个可清理的
         *   临时目录，避免与正式数据混用。
         *   示例：bucket/tmp/issue1277/debug-shuffle/
         *
         * PIXELS_S3QS_DEBUG_QUEUE_NAME
         *   partition 0 对应的 SQS 队列名称。WorkerCommon 根据 ShuffleInfo
         *   初始化 shuffle storage 时，S3QS 存储实现会通过 S3QS.registerQueue(...)
         *   解析或创建该队列。
         *
         * 可选环境变量：
         *
         * PIXELS_S3QS_DEBUG_INPUT_SCHEME
         *   输入数据的存储协议，默认值为 s3。调试其它存储后端时，可以改成
         *   Storage.Scheme 支持的协议名称。
         *
         * PIXELS_S3QS_DEBUG_COLUMNS
         *   需要读取的列名列表，使用逗号分隔，默认值为 c0。这里会同时影响
         *   ScanTableInfo 的读取列和 PartitionInput 的投影数组长度。
         *
         * 测试启动一个本地 WorkerCoordinateServer，然后把真实的 Spike worker
         * 请求直接传入 RequestHandlerImpl。这样可以绕开外部 Spike 网络服务，
         * 但仍然执行 worker 侧真实的请求反序列化、类型分发、WorkerService
         * 调用以及后续 S3QS partition stage worker 逻辑。
         */
        long transId = 1277001L;
        long timestamp = System.currentTimeMillis();
        int coordinatorPort = 18887;

        String inputPath = requireEnv("PIXELS_S3QS_DEBUG_INPUT_PATH");
        String objectPrefix = requireEnv("PIXELS_S3QS_DEBUG_OBJECT_PREFIX");
        String queueName = requireEnv("PIXELS_S3QS_DEBUG_QUEUE_NAME");
        Storage.Scheme inputScheme = Storage.Scheme.from(
                getEnvOrDefault("PIXELS_S3QS_DEBUG_INPUT_SCHEME", "s3"));
        String[] columnsToRead = getEnvOrDefault("PIXELS_S3QS_DEBUG_COLUMNS", "c0").split(",");

        WorkerCoordinateServer coordinateServer = new WorkerCoordinateServer(coordinatorPort);
        Thread coordinateServerThread = new Thread(coordinateServer, "issue1277-worker-coordinate-server");
        coordinateServerThread.setDaemon(true);
        coordinateServerThread.start();
        Thread.sleep(500L);

        try
        {
            PartitionInput smallPartitionInput = createProducerInput(transId, timestamp,
                    inputPath, inputScheme, columnsToRead, objectPrefix, queueName);
            PartitionInput largePartitionInput = createProducerInput(transId, timestamp,
                    inputPath, inputScheme, columnsToRead, objectPrefix, queueName);
            PartitionedJoinInput joinInput = createJoinInput(transId, timestamp, objectPrefix, queueName);

            PartitionedJoinS3QSOperator operator = new PartitionedJoinS3QSOperator("issue1277-s3qs-debug",
                    Collections.singletonList(smallPartitionInput),
                    Collections.singletonList(largePartitionInput),
                    Collections.<JoinInput>singletonList(joinInput),
                    JoinAlgorithm.PARTITIONED);
            PlanCoordinatorFactory.Instance().createPlanCoordinator(transId, operator);

            /*
             * 生产端 stage id 不是在构造 PartitionInput 时手工指定的，而是在
             * PlanCoordinatorFactory.createPlanCoordinator(...) 内部调用
             * initPlanCoordinator(...) 时分配的。该过程会把 stage id 回写到原始
             * PartitionInput 对象中，因此这里直接从 smallPartitionInput 读取，
             * 确保发给 worker 的 StageWorkerInput 与协调器中的 stage 定义一致。
             */
            StageWorkerInput stageWorkerInput = new StageWorkerInput(transId, timestamp,
                    smallPartitionInput.getStageId(), operator.getName(), WorkerType.PARTITION_S3QS);
            stageWorkerInput.setCoordinatorHost("localhost");
            stageWorkerInput.setCoordinatorPort(coordinatorPort);

            SpikeWorkerRequest workerRequest = new SpikeWorkerRequest(WorkerType.PARTITION_S3QS,
                    JSON.toJSONString(stageWorkerInput, SerializerFeature.DisableCircularReferenceDetect));
            SpikeWorker.CallWorkerFunctionReq request = SpikeWorker.CallWorkerFunctionReq.newBuilder()
                    .setRequestId(1277L)
                    .setPayload(JSON.toJSONString(workerRequest, SerializerFeature.DisableCircularReferenceDetect))
                    .build();

            /*
             * 建议从下一行开始设置断点。执行 new RequestHandlerImpl().execute(request)
             * 后，可以观察 Spike worker 请求进入真实 S3QS partition worker 的完整链路：
             *
             * RequestHandlerImpl.execute(request)
             *   -> switch PARTITION_S3QS，根据 WorkerType 选择 S3QS partition 分支
             *   -> WorkerService<S3QSPartitionWorker, StageWorkerInput, PartitionOutput>
             *   -> S3QSPartitionWorker.handleRequest(...)，进入 S3QS 分区 worker
             *   -> S3QSStageWorkerRunner.runPartition(...)，启动 stage worker runner
             *   -> WorkerCoordinateService.registerWorker(...)，向本地协调器注册 worker
             *   -> WorkerCoordinateService.getTasksToExecute(...)，从协调器领取任务
             *   -> BasePartitionWorker.process(PartitionInput)，执行 partition 输入处理
             *   -> S3QS 生产端数据路径，将 shuffle 数据写入 S3QS/S3/SQS
             */
            SpikeWorker.CallWorkerFunctionResp response = new RequestHandlerImpl().execute(request);

            assertEquals(1277L, response.getRequestId());
            System.out.println(response.getPayload());
        }
        finally
        {
            coordinateServer.shutdown();
        }
    }

    private static PartitionInput createProducerInput(long transId, long timestamp, String inputPath,
                                                      Storage.Scheme inputScheme, String[] columnsToRead,
                                                      String objectPrefix, String queueName)
    {
        ScanTableInfo tableInfo = new ScanTableInfo("issue1277_debug", true, columnsToRead,
                new StorageInfo(inputScheme, null, null, null, null),
                Collections.singletonList(new InputSplit(Collections.singletonList(new InputInfo(inputPath, 0, 1)))),
                null);
        ShuffleInfo shuffleInfo = createShuffleInfo(objectPrefix, queueName);
        OutputInfo outputInfo = new OutputInfo(objectPrefix + "producer-output",
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null), true);
        outputInfo.setShuffleInfo(shuffleInfo);
        PartitionInput partitionInput = new PartitionInput(transId, timestamp, tableInfo,
                allProjected(columnsToRead.length), outputInfo, new PartitionInfo(new int[] {0}, 1));
        partitionInput.setProducerTaskId(0);
        return partitionInput;
    }

    private static PartitionedJoinInput createJoinInput(long transId, long timestamp,
                                                        String objectPrefix, String queueName)
    {
        PartitionedJoinInput joinInput = new PartitionedJoinInput();
        joinInput.setTransId(transId);
        joinInput.setTimestamp(timestamp);
        joinInput.setSmallTable(createPartitionedTableInfo(objectPrefix, queueName));
        joinInput.setLargeTable(createPartitionedTableInfo(objectPrefix, queueName));
        return joinInput;
    }

    private static PartitionedTableInfo createPartitionedTableInfo(String objectPrefix, String queueName)
    {
        PartitionedTableInfo tableInfo = new PartitionedTableInfo();
        tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3qs, null, null, null, null));
        tableInfo.setShuffleInfo(createShuffleInfo(objectPrefix, queueName));
        return tableInfo;
    }

    private static ShuffleInfo createShuffleInfo(String objectPrefix, String queueName)
    {
        return new ShuffleInfo("issue1277-debug-shuffle",
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                objectPrefix, 1, 1, 1, 1,
                Collections.singletonList(new ShuffleQueueInfo(0, queueName, null)));
    }

    private static boolean[] allProjected(int columnCount)
    {
        boolean[] projection = new boolean[columnCount];
        for (int i = 0; i < projection.length; ++i)
        {
            projection[i] = true;
        }
        return projection;
    }

    private static String requireEnv(String name)
    {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty())
        {
            throw new IllegalStateException("missing environment variable: " + name);
        }
        return value;
    }

    private static String getEnvOrDefault(String name, String defaultValue)
    {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty())
        {
            return defaultValue;
        }
        return value;
    }
}
