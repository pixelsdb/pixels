/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.worker.vhive;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.TaskBatch;
import io.pixelsdb.pixels.planner.coordinate.TaskInfo;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateService;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.worker.common.*;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitionStreamWorker extends BasePartitionWorker implements RequestHandler<PartitionInput, PartitionOutput>
{
    protected WorkerCoordinateService workerCoordinatorService;
    private io.pixelsdb.pixels.common.task.Worker<CFWorkerInfo> worker;
    private List<CFWorkerInfo> downStreamWorkers;
    private final WorkerMetrics workerMetrics;
    private final Logger logger;


    public PartitionStreamWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    @Override
    public PartitionOutput handleRequest(PartitionInput input)
    {
        long startTime = System.currentTimeMillis();
        try {
            int stageId = input.getStageId();
            long transId = input.getTransId();
            String ip = WorkerCommon.getIpAddress();
            int port = WorkerCommon.getPort();
            String coordinatorIp = WorkerCommon.getCoordinatorIp();
            int coordinatorPort = WorkerCommon.getCoordinatorPort();
            CFWorkerInfo workerInfo = new CFWorkerInfo(ip, port, transId, stageId, Constants.PARTITION_OPERATOR_NAME, Collections.emptyList());
            workerCoordinatorService = new WorkerCoordinateService(coordinatorIp, coordinatorPort);
            worker = workerCoordinatorService.registerWorker(workerInfo);
            downStreamWorkers = workerCoordinatorService.getDownstreamWorkers(worker.getWorkerId());
            checkArgument(!downStreamWorkers.isEmpty(),
                    "at least one downstream worker is allowed");
            PartitionOutput output = process(input);
            workerCoordinatorService.terminateWorker(worker.getWorkerId());
            workerCoordinatorService.shutdown();
            return output;
        } catch (Throwable e) {
            PartitionOutput partitionOutput = new PartitionOutput();
            this.logger.error("error during registering worker", e);
            partitionOutput.setSuccessful(false);
            partitionOutput.setErrorMessage(e.getMessage());
            partitionOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return partitionOutput;
        }
    }

    @Override
    public String getRequestId()
    {
        return this.context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType()
    {
        return WorkerType.PARTITION_STREAMING;
    }

    @Override
    public PartitionOutput process(PartitionInput event)
    {
        PartitionOutput partitionOutput = new PartitionOutput();
        long startTime = System.currentTimeMillis();
        partitionOutput.setStartTimeMs(startTime);
        partitionOutput.setRequestId(context.getRequestId());
        partitionOutput.setSuccessful(true);
        partitionOutput.setErrorMessage("");
        workerMetrics.clear();

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            long timestamp = event.getTimestamp();
            requireNonNull(event.getTableInfo(), "event.tableInfo is null");
            StorageInfo inputStorageInfo = event.getTableInfo().getStorageInfo();
            requireNonNull(event.getPartitionInfo(), "event.partitionInfo is null");
            int numPartition = event.getPartitionInfo().getNumPartition();
            logger.info("table '" + event.getTableInfo().getTableName() +
                    "', number of partitions (" + numPartition + ")");
            int[] keyColumnIds = event.getPartitionInfo().getKeyColumnIds();
            boolean[] projection = event.getProjection();
            requireNonNull(event.getOutput(), "event.output is null");
            StorageInfo outputStorageInfo = requireNonNull(event.getOutput().getStorageInfo(),
                    "output.storageInfo is null");
            checkArgument(event.getOutput().getStorageInfo().getScheme().equals(Storage.Scheme.httpstream));
            List<String> outputPaths = new ArrayList<>(numPartition);
            for (int i = 0; i < numPartition; i++)
            {
                outputPaths.add(downStreamWorkers.get(i).getIp() + ":" + (downStreamWorkers.get(i).getPort() + worker.getWorkerPortIndex()));
            }
            boolean encoding = event.getOutput().isEncoding();

            WorkerCommon.initStorage(inputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            String[] columnsToRead = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);
            AtomicReference<TypeDescription> writerSchema = new AtomicReference<>();
            List<Future> partitionFutures = new ArrayList<>();
            // The partitioned data would be kept in memory.
            List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitioned = new ArrayList<>(numPartition);
            for (int i = 0; i < numPartition; ++i)
            {
                partitioned.add(new ConcurrentLinkedQueue<>());
            }

            // get tasks from worker coordinator
            TaskBatch taskBatch = workerCoordinatorService.getTasksToExecute(worker.getWorkerId());
            while (!taskBatch.isEndOfTasks())
            {
                List<TaskInfo> taskInfos = taskBatch.getTasks();
                List<InputSplit> inputSplits = new ArrayList<>();
                for (TaskInfo taskInfo : taskInfos)
                {
                    PartitionInput input = JSON.parseObject(taskInfo.getPayload(), PartitionInput.class);
                    requireNonNull(input.getTableInfo(), "task.tableInfo is null");
                    inputSplits.addAll(input.getTableInfo().getInputSplits());
                }
                if (writerSchema.get() == null)
                {
                    TypeDescription fileSchema = WorkerCommon.getFileSchemaFromSplits(
                            WorkerCommon.getStorage(inputStorageInfo.getScheme()), inputSplits);
                    TypeDescription resultSchema = WorkerCommon.getResultSchema(fileSchema, columnsToRead);
                    writerSchema.set(resultSchema);
                }
                for (InputSplit inputSplit : inputSplits)
                {
                    List<InputInfo> scanInputs = inputSplit.getInputInfos();

                    partitionFutures.add(threadPool.submit(() -> {
                                try
                                {
                                    partitionFile(transId, timestamp, scanInputs, columnsToRead, inputStorageInfo.getScheme(),
                                            filter, keyColumnIds, projection, partitioned, writerSchema);
                                }
                                catch (Throwable e)
                                {
                                    throw new WorkerException("error during partitioning", e);
                                }
                            }
                    ));
                }
                for (Future future : partitionFutures)
                {
                    future.get();
                }
                workerCoordinatorService.completeTasks(worker.getWorkerId(), taskInfos);
                taskBatch = workerCoordinatorService.getTasksToExecute(worker.getWorkerId());
            }

            // write to down stream workers
            WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
            Set<Integer> hashValues = new HashSet<>(numPartition);
            for (int hash = 0; hash < numPartition; hash++)
            {
                int finalHash = hash;
                threadPool.execute(() -> {
                    PixelsWriter pixelsWriter = WorkerCommon.getWriter(writerSchema.get(),
                            WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPaths.get(finalHash), encoding,
                            true, Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));
                    ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitioned.get(finalHash);
                    int size = 0;
                    if (!batches.isEmpty())
                    {
                        for (VectorizedRowBatch batch : batches)
                        {
                            size += batch.size;
                            try
                            {
                                pixelsWriter.addRowBatch(batch, finalHash);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        hashValues.add(finalHash);
                    }
                    logger.info("partition {} has {} rows", finalHash, size);
                    try
                    {
                        pixelsWriter.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                    workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                throw new WorkerException("interrupted while waiting for the termination of partitioning", e);
            }
            if (exceptionHandler.hasException())
            {
                throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
            }
            outputPaths.forEach(partitionOutput::addOutput);
            partitionOutput.setHashValues(hashValues);

            workerMetrics.addOutputCostNs(writeCostTimer.stop());
            partitionOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(partitionOutput, workerMetrics);
            return partitionOutput;
        }
        catch (Throwable e)
        {
            logger.error("error during partition", e);
            partitionOutput.setSuccessful(false);
            partitionOutput.setErrorMessage(e.getMessage());
            partitionOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return partitionOutput;
        }
    }
}
