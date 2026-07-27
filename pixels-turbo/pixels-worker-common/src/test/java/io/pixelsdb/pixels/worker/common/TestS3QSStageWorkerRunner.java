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
package io.pixelsdb.pixels.worker.common;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.lease.Lease;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.TaskBatch;
import io.pixelsdb.pixels.planner.coordinate.TaskInfo;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateService;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TestS3QSStageWorkerRunner
{
    @Test
    public void coordinatorLifecycleExecutesCompletesAndTerminatesTask() throws Exception
    {
        WorkerCoordinateService coordinateService = mock(WorkerCoordinateService.class);
        WorkerContext context = new WorkerContext(LogManager.getLogger(TestS3QSStageWorkerRunner.class),
                new WorkerMetrics(), "request-1277");
        S3QSStageWorkerRunner runner = new S3QSStageWorkerRunner(context, coordinateService);

        long workerId = 17L;
        long transId = 1277L;
        int stageId = 3;
        Worker<CFWorkerInfo> runtimeWorker = new Worker<>(workerId,
                new Lease(System.currentTimeMillis(), 60000L), 0,
                new CFWorkerInfo("localhost", -1, transId, stageId,
                        Constants.PARTITION_OPERATOR_NAME, Collections.emptyList()));
        TaskInfo taskInfo = new TaskInfo(5, JSON.toJSONString("task-payload"));
        List<TaskInfo> taskInfos = Collections.singletonList(taskInfo);
        TaskBatch taskBatch = new TaskBatch(false, taskInfos);
        TaskBatch endOfTasks = new TaskBatch(true, Collections.emptyList());

        when(coordinateService.registerWorker(any(CFWorkerInfo.class))).thenReturn(runtimeWorker);
        when(coordinateService.getTasksToExecute(workerId)).thenReturn(taskBatch, endOfTasks);

        PartitionOutput taskOutput = new PartitionOutput();
        taskOutput.setSuccessful(true);
        taskOutput.addOutput("s3://bucket/shuffle/task-5");
        taskOutput.setNumReadRequests(2);
        taskOutput.setNumWriteRequests(1);
        taskOutput.setTotalReadBytes(128L);
        taskOutput.setTotalWriteBytes(64L);
        taskOutput.setHashValues(Collections.singleton(9));
        AtomicReference<Object> executedPayload = new AtomicReference<>();

        Class<?> taskExecutorClass = Class.forName(
                "io.pixelsdb.pixels.worker.common.S3QSStageWorkerRunner$TaskExecutor");
        Object taskExecutor = Proxy.newProxyInstance(taskExecutorClass.getClassLoader(),
                new Class<?>[] {taskExecutorClass}, (proxy, method, args) ->
                {
                    if (method.getName().equals("execute"))
                    {
                        executedPayload.set(args[0]);
                        return taskOutput;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        Method run = S3QSStageWorkerRunner.class.getDeclaredMethod("run",
                StageWorkerInput.class, String.class, WorkerType.class, Class.class,
                taskExecutorClass, Output.class);
        run.setAccessible(true);

        StageWorkerInput input = new StageWorkerInput(transId, 1000L, stageId,
                "test-s3qs-stage", WorkerType.PARTITION_S3QS);
        PartitionOutput aggregateOutput = (PartitionOutput) run.invoke(runner, input,
                Constants.PARTITION_OPERATOR_NAME, WorkerType.PARTITION_S3QS,
                String.class, taskExecutor, new PartitionOutput());

        assertEquals("task-payload", executedPayload.get());
        assertTrue(taskInfo.isSuccess());
        assertTrue(aggregateOutput.isSuccessful());
        assertEquals("request-1277", aggregateOutput.getRequestId());
        assertEquals(Collections.singletonList("s3://bucket/shuffle/task-5"), aggregateOutput.getOutputs());
        assertEquals(2, aggregateOutput.getNumReadRequests());
        assertEquals(1, aggregateOutput.getNumWriteRequests());
        assertEquals(128L, aggregateOutput.getTotalReadBytes());
        assertEquals(64L, aggregateOutput.getTotalWriteBytes());
        assertEquals(Collections.singleton(9), aggregateOutput.getHashValues());

        ArgumentCaptor<CFWorkerInfo> workerInfo = ArgumentCaptor.forClass(CFWorkerInfo.class);
        InOrder calls = inOrder(coordinateService);
        calls.verify(coordinateService).registerWorker(workerInfo.capture());
        calls.verify(coordinateService).getTasksToExecute(workerId);
        calls.verify(coordinateService).completeTasks(workerId, taskInfos);
        calls.verify(coordinateService).getTasksToExecute(workerId);
        calls.verify(coordinateService).terminateWorker(workerId);

        assertEquals(transId, workerInfo.getValue().getTransId());
        assertEquals(stageId, workerInfo.getValue().getStageId());
        assertEquals(Constants.PARTITION_OPERATOR_NAME, workerInfo.getValue().getOperatorName());
    }

    @Test
    public void rejectsUnexpectedWorkerTypeBeforeRegistering() throws Exception
    {
        WorkerCoordinateService coordinateService = mock(WorkerCoordinateService.class);
        WorkerContext context = new WorkerContext(LogManager.getLogger(TestS3QSStageWorkerRunner.class),
                new WorkerMetrics(), "request-1277");
        S3QSStageWorkerRunner runner = new S3QSStageWorkerRunner(context, coordinateService);
        StageWorkerInput input = new StageWorkerInput(1277L, 1000L, 3,
                "test-s3qs-stage", WorkerType.PARTITIONED_JOIN_S3QS);

        try
        {
            runner.runPartition(input);
            fail("expected WorkerException");
        }
        catch (WorkerException e)
        {
            assertTrue(e.getMessage().contains("unexpected S3QS stage worker type"));
        }

        verifyNoInteractions(coordinateService);
    }
}
