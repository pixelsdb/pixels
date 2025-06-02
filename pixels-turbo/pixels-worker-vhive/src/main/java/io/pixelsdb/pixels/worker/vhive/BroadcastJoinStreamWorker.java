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
import io.pixelsdb.pixels.executor.join.HashJoiner;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateService;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
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

public class BroadcastJoinStreamWorker extends BaseBroadcastJoinWorker implements RequestHandler<BroadcastJoinInput, JoinOutput>
{
    private final Logger logger;
    protected WorkerCoordinateService workerCoordinatorService;
    private final WorkerMetrics workerMetrics;
    private io.pixelsdb.pixels.common.task.Worker<CFWorkerInfo> worker;
    private List<CFWorkerInfo> downStreamWorkers;

    public BroadcastJoinStreamWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    @Override
    public JoinOutput handleRequest(BroadcastJoinInput input)
    {
        long startTime = System.currentTimeMillis();
        try {
            int stageId = input.getStageId();
            long transId = input.getTransId();
            String ip = WorkerCommon.getIpAddress();
            int port = WorkerCommon.getPort();
            String coordinatorIp = WorkerCommon.getCoordinatorIp();
            int coordinatorPort = WorkerCommon.getCoordinatorPort();
            CFWorkerInfo workerInfo = new CFWorkerInfo(ip, port, transId, stageId,
                    Constants.BROADCAST_OPERATOR_NAME, Collections.emptyList());
            workerCoordinatorService = new WorkerCoordinateService(coordinatorIp, coordinatorPort);
            worker = workerCoordinatorService.registerWorker(workerInfo);
            JoinOutput output = process(input);
            workerCoordinatorService.terminateWorker(worker.getWorkerId());
            workerCoordinatorService.shutdown();
            return output;
        } catch (Throwable e) {
            JoinOutput joinOutput = new JoinOutput();
            this.logger.error("error during registering worker", e);
            joinOutput.setSuccessful(false);
            joinOutput.setErrorMessage(e.getMessage());
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return joinOutput;
        }
    }

    @Override
    public String getRequestId() {
        return this.context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType() {
        return WorkerType.BROADCAST_JOIN_STREAMING;
    }

    @Override
    public JoinOutput process(BroadcastJoinInput input)
    {
        JoinOutput joinOutput = new JoinOutput();
        long startTime = System.currentTimeMillis();
        joinOutput.setStartTimeMs(startTime);
        joinOutput.setRequestId(context.getRequestId());
        joinOutput.setSuccessful(true);
        joinOutput.setErrorMessage("");

        try {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = input.getTransId();
            long timestamp = input.getTimestamp();
            BroadcastTableInfo leftTable = requireNonNull(input.getSmallTable(), "leftTable is null");
            StorageInfo leftInputStorageInfo = requireNonNull(leftTable.getStorageInfo(), "leftStorageInfo is null");
            List<InputSplit> leftInputs = requireNonNull(leftTable.getInputSplits(), "leftInputs is null");
            checkArgument(leftInputs.size() > 0, "left table is empty");
            String[] leftColumnsToRead = leftTable.getColumnsToRead();
            int[] leftKeyColumnIds = leftTable.getKeyColumnIds();
            TableScanFilter leftFilter = JSON.parseObject(leftTable.getFilter(), TableScanFilter.class);

            BroadcastTableInfo rightTable = requireNonNull(input.getLargeTable(), "rightTable is null");
            StorageInfo rightInputStorageInfo = requireNonNull(rightTable.getStorageInfo(), "rightStorageInfo is null");
            List<InputSplit> rightInputs = requireNonNull(rightTable.getInputSplits(), "rightInputs is null");
            checkArgument(rightInputs.size() > 0, "right table is empty");
            String[] rightColumnsToRead = rightTable.getColumnsToRead();
            int[] rightKeyColumnIds = rightTable.getKeyColumnIds();
            TableScanFilter rightFilter = JSON.parseObject(rightTable.getFilter(), TableScanFilter.class);

            if (leftInputStorageInfo.getScheme() == Storage.Scheme.httpstream ||
                    rightInputStorageInfo.getScheme() == Storage.Scheme.httpstream)
            {
                int port = WorkerCommon.getPort();
                if (leftInputStorageInfo.getScheme() == Storage.Scheme.httpstream)
                {
                    for (InputSplit inputSplit : leftInputs)
                    {
                        checkArgument(inputSplit.getInputInfos().size() == 1,
                                "inputSplit has more than one input");
                        inputSplit.getInputInfos().get(0).setPath("localhost:" + port);
                        port++;
                    }
                }
                if (rightInputStorageInfo.getScheme() == Storage.Scheme.httpstream)
                {
                    for (InputSplit inputSplit : rightInputs)
                    {
                        checkArgument(inputSplit.getInputInfos().size() == 1,
                                "inputSplit has more than one input");
                        inputSplit.getInputInfos().get(0).setPath("localhost:" + port);
                        port++;
                    }
                }
            }

            String[] leftColAlias = input.getJoinInfo().getSmallColumnAlias();
            String[] rightColAlias = input.getJoinInfo().getLargeColumnAlias();
            boolean[] leftProjection = input.getJoinInfo().getSmallProjection();
            boolean[] rightProjection = input.getJoinInfo().getLargeProjection();
            JoinType joinType = input.getJoinInfo().getJoinType();
            checkArgument(joinType != JoinType.EQUI_LEFT && joinType != JoinType.EQUI_FULL,
                    "broadcast join can not be used for LEFT_OUTER or FULL_OUTER join");
            logger.info("small table: " + input.getSmallTable().getTableName() +
                    ", large table: " + input.getLargeTable().getTableName());

            MultiOutputInfo outputInfo = input.getOutput();
            StorageInfo outputStorageInfo = outputInfo.getStorageInfo();
            checkArgument(outputInfo.getFileNames().size() == 1,
                    "it is incorrect to have more than one output files");
            String outputFolder = outputInfo.getPath();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = outputInfo.isEncoding();

            WorkerCommon.initStorage(leftInputStorageInfo);
            WorkerCommon.initStorage(rightInputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            boolean partitionOutput = input.getJoinInfo().isPostPartition();
            PartitionInfo outputPartitionInfo = input.getJoinInfo().getPostPartitionInfo();
            if (partitionOutput) {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }

            // build the joiner
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            WorkerCommon.getFileSchemaFromSplits(threadPool,
                    WorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                    WorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                    leftSchema, rightSchema, leftInputs, rightInputs);
            HashJoiner joiner = new HashJoiner(joinType,
                    WorkerCommon.getResultSchema(leftSchema.get(), leftColumnsToRead),
                    leftColAlias, leftProjection, leftKeyColumnIds,
                    WorkerCommon.getResultSchema(rightSchema.get(), rightColumnsToRead),
                    rightColAlias, rightProjection, rightKeyColumnIds);
            logger.info("succeed to get left and right schema");

            // build the hash table for the left table.
            List<Future> leftFutures = new ArrayList<>();
            for (InputSplit inputSplit : leftInputs)
            {
                List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                leftFutures.add(threadPool.submit(() -> {
                    try
                    {
                        buildHashTable(transId, timestamp, joiner, inputs, leftInputStorageInfo.getScheme(),
                                !leftTable.isBase(), leftColumnsToRead, leftFilter, workerMetrics);
                    }
                    catch (Throwable e)
                    {
                        throw new WorkerException("error during hash table construction", e);
                    }
                }));
            }
            for (Future future : leftFutures)
            {
                future.get();
            }
            logger.info("hash table size: " + joiner.getSmallTableSize() + ", duration (ns): " +
                    (workerMetrics.getInputCostNs() + workerMetrics.getComputeCostNs()));

            List<ConcurrentLinkedQueue<VectorizedRowBatch>> result = new ArrayList<>();
            if (partitionOutput)
            {
                for (int i = 0; i < outputPartitionInfo.getNumPartition(); ++i)
                {
                    result.add(new ConcurrentLinkedQueue<>());
                }
            }
            else
            {
                result.add(new ConcurrentLinkedQueue<>());
            }

            List<Future> rightFutures = new ArrayList<>();
            if (joiner.getSmallTableSize() > 0)
            {
                for (InputSplit inputSplit : rightInputs)
                {
                    List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                    rightFutures.add(threadPool.submit(() -> {
                        try
                        {
                            int numJoinedRows = partitionOutput ?
                                    joinWithRightTableAndPartition(
                                            transId, timestamp, joiner, inputs, rightInputStorageInfo.getScheme(),
                                            !rightTable.isBase(), rightColumnsToRead, rightFilter,
                                            outputPartitionInfo, result, workerMetrics) :
                                    joinWithRightTable(transId, timestamp, joiner, inputs, rightInputStorageInfo.getScheme(),
                                            !rightTable.isBase(), rightColumnsToRead, rightFilter, result.get(0), workerMetrics);
                        } catch (Throwable e)
                        {
                            throw new WorkerException("error during broadcast join", e);
                        }
                    }));
                }
                for (Future future : rightFutures)
                {
                    future.get();
                }
            }

            logger.info("write to down stream workers");
            downStreamWorkers = workerCoordinatorService.getDownstreamWorkers(worker.getWorkerId());
            List<String> outputPaths = new ArrayList<>();
            if (outputStorageInfo.getScheme() == Storage.Scheme.httpstream)
            {
                int port = worker.getWorkerPortIndex();
                for (CFWorkerInfo workerInfo : downStreamWorkers)
                {
                    outputPaths.add(workerInfo.getIp() + ":" + (workerInfo.getPort() + port));
                }
            } else
            {
                outputPaths.add(outputFolder + outputInfo.getFileNames().get(0));
            }
            try
            {
                WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                if (outputStorageInfo.getScheme() == Storage.Scheme.httpstream)
                {
                    if (partitionOutput)
                    {
                        checkArgument(outputPartitionInfo.getNumPartition() == downStreamWorkers.size(),
                                "a worker should corresponds to one partition");
                    }
                    for (int i = 0; i < outputPaths.size(); i++)
                    {
                        logger.info("write to path {}", outputPaths.get(i));
                        int finalI = i;
                        threadPool.submit(() -> {
                            PixelsWriter pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                                    WorkerCommon.getStorage(outputStorageInfo.getScheme()),
                                    outputPaths.get(finalI), encoding, false, Arrays.stream(
                                                    outputPartitionInfo.getKeyColumnIds()).boxed().
                                            collect(Collectors.toList()));
                            ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches;
                            if (partitionOutput)
                            {
                                rowBatches = result.get(finalI);
                            } else
                            {
                                rowBatches = result.get(0);
                            }
                            for (VectorizedRowBatch rowBatch : rowBatches)
                            {
                                try
                                {
                                    pixelsWriter.addRowBatch(rowBatch);
                                } catch (IOException e)
                                {
                                    throw new RuntimeException(e);
                                }
                            }
                            try
                            {
                                pixelsWriter.close();
                            } catch (IOException e)
                            {
                                throw new RuntimeException(e);
                            }
                            joinOutput.addOutput(outputPaths.get(0), pixelsWriter.getNumRowGroup());
                            workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                            workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                        });
                    }
                } else
                {
                    PixelsWriter pixelsWriter;
                    if (partitionOutput)
                    {
                        pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                                WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPaths.get(0),
                                encoding, true, Arrays.stream(
                                                outputPartitionInfo.getKeyColumnIds()).boxed().
                                        collect(Collectors.toList()));
                        for (int hash = 0; hash < outputPartitionInfo.getNumPartition(); ++hash)
                        {
                            ConcurrentLinkedQueue<VectorizedRowBatch> batches = result.get(hash);
                            if (!batches.isEmpty())
                            {
                                for (VectorizedRowBatch batch : batches)
                                {
                                    pixelsWriter.addRowBatch(batch, hash);
                                }
                            }
                        }
                    }
                    else
                    {
                        pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                                WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPaths.get(0),
                                encoding, false, null);
                        ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result.get(0);
                        for (VectorizedRowBatch rowBatch : rowBatches)
                        {
                            pixelsWriter.addRowBatch(rowBatch);
                        }
                    }
                    pixelsWriter.close();
                    joinOutput.addOutput(outputPaths.get(0), pixelsWriter.getNumRowGroup());
                    workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                    workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                }

                if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPaths.get(0)))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }
                workerMetrics.addOutputCostNs(writeCostTimer.stop());
            } catch (Throwable e)
            {
                throw new WorkerException(
                        "failed to finish writing and close the join result file '" +
                                String.join(",", outputPaths) + "'", e);
            }

            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) ;
            } catch (InterruptedException e)
            {
                throw new WorkerException("interrupted while waiting for the termination of join", e);
            }

            if (exceptionHandler.hasException())
            {
                throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
            }
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(joinOutput, workerMetrics);
            return joinOutput;
        } catch (Throwable e) {
            logger.error("error during join", e);
            joinOutput.setSuccessful(false);
            joinOutput.setErrorMessage(e.getMessage());
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return joinOutput;
        }
    }
}