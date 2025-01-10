/*
 * Copyright 2025 PixelsDB.
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

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateService;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BasePartitionedChainJoinStreamWorker extends Worker<PartitionedChainJoinInput, JoinOutput>
{
    protected WorkerCoordinateService workerCoordinatorService;
    protected io.pixelsdb.pixels.common.task.Worker<CFWorkerInfo> worker;
    protected List<CFWorkerInfo> downStreamWorkers;
    protected final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BasePartitionedChainJoinStreamWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    @Override
    public JoinOutput process(PartitionedChainJoinInput event)
    {
        JoinOutput joinOutput = new JoinOutput();
        long startTime = System.currentTimeMillis();
        joinOutput.setStartTimeMs(startTime);
        joinOutput.setRequestId(context.getRequestId());
        joinOutput.setSuccessful(true);
        joinOutput.setErrorMessage("");

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            logger.info("This is " + worker.getWorkerInfo().getIp());
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            long timestamp = event.getTimestamp();
            List<BroadcastTableInfo> chainTables = event.getChainTables();
            List<ChainJoinInfo> chainJoinInfos = event.getChainJoinInfos();
            requireNonNull(chainTables, "leftTables is null");
            requireNonNull(chainJoinInfos, "chainJoinInfos is null");
            checkArgument(chainTables.size() == chainJoinInfos.size(),
                    "left table num is not consistent with chain-join info num.");
            checkArgument(chainTables.size() > 1, "there should be at least two chain tables");

            requireNonNull(event.getSmallTable(), "leftTable is null");
            StorageInfo leftInputStorageInfo = requireNonNull(event.getSmallTable().getStorageInfo(),
                    "leftInputStorageInfo is null");
            List<String> leftPartitioned = requireNonNull( event.getSmallTable().getInputFiles(),
                    "leftPartitioned is null");
            checkArgument(leftPartitioned.size() > 0, "leftPartitioned is empty");
            int leftParallelism = event.getSmallTable().getParallelism();
            checkArgument(leftParallelism > 0, "leftParallelism is not positive");
            String[] leftColumnsToRead = event.getSmallTable().getColumnsToRead();
            int[] leftKeyColumnIds = event.getSmallTable().getKeyColumnIds();

            requireNonNull(event.getLargeTable(), "rightTable is null");
            StorageInfo rightInputStorageInfo = requireNonNull(event.getLargeTable().getStorageInfo(),
                    "rightInputStorageInfo is null");
            List<String> rightPartitioned = requireNonNull(event.getLargeTable().getInputFiles(),
                    "rightPartitioned is null");
            checkArgument(rightPartitioned.size() > 0, "rightPartitioned is empty");
            int rightParallelism = event.getLargeTable().getParallelism();
            checkArgument(rightParallelism > 0, "rightParallelism is not positive");
            String[] rightColumnsToRead = event.getLargeTable().getColumnsToRead();
            int[] rightKeyColumnIds = event.getLargeTable().getKeyColumnIds();

            if (leftInputStorageInfo.getScheme().equals(Storage.Scheme.httpstream) ||
                    rightInputStorageInfo.getScheme().equals(Storage.Scheme.httpstream))
            {
                int port = WorkerCommon.getPort();
                if (leftInputStorageInfo.getScheme().equals(Storage.Scheme.httpstream))
                {
                    for (int i = 0; i < leftPartitioned.size(); i++)
                    {
                        leftPartitioned.set(i, "localhost:" + port);
                        port++;
                    }
                }
                if (rightInputStorageInfo.getScheme().equals(Storage.Scheme.httpstream))
                {
                    for (int i = 0; i < rightPartitioned.size(); i++)
                    {
                        rightPartitioned.set(i, "localhost:" + port);
                        port++;
                    }
                }
            }

            String[] leftColAlias = event.getJoinInfo().getSmallColumnAlias();
            String[] rightColAlias = event.getJoinInfo().getLargeColumnAlias();
            boolean[] leftProjection = event.getJoinInfo().getSmallProjection();
            boolean[] rightProjection = event.getJoinInfo().getLargeProjection();
            JoinType joinType = event.getJoinInfo().getJoinType();
            checkArgument(joinType != JoinType.EQUI_LEFT && joinType != JoinType.EQUI_FULL,
                    "currently, left or full outer join is not supported in partitioned chain join");
            List<Integer> hashValues = event.getJoinInfo().getHashValues();
            int numPartition = event.getJoinInfo().getNumPartition();
            logger.info("small table: " + event.getSmallTable().getTableName() +
                    ", large table: " + event.getLargeTable().getTableName() +
                    ", number of partitions (" + numPartition + ")");

            MultiOutputInfo outputInfo = event.getOutput();
            StorageInfo outputStorageInfo = outputInfo.getStorageInfo();
            checkArgument(outputInfo.getFileNames().size() == 1,
                    "it is incorrect to have more than one output files");
            String outputFolder = outputInfo.getPath();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = outputInfo.isEncoding();

            ChainJoinInfo lastJoin = chainJoinInfos.get(chainJoinInfos.size() - 1);
            boolean partitionOutput = lastJoin.isPostPartition();
            PartitionInfo outputPartitionInfo = lastJoin.getPostPartitionInfo();

            if (partitionOutput)
            {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }

            for (TableInfo tableInfo : chainTables)
            {
                WorkerCommon.initStorage(tableInfo.getStorageInfo());
            }
            WorkerCommon.initStorage(leftInputStorageInfo);
            WorkerCommon.initStorage(rightInputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            // build the joiner
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            WorkerCommon.getFileSchemaFromPaths(threadPool,
                    WorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                    WorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                    leftSchema, rightSchema, leftPartitioned, rightPartitioned);
            /*
             * Issue #450:
             * For the left and the right partial partitioned files, the file schema is equal to the columns to read in normal cases.
             * However, it is safer to turn file schema into result schema here.
             */
            Joiner partitionJoiner = new Joiner(joinType,
                    WorkerCommon.getResultSchema(leftSchema.get(), leftColumnsToRead), leftColAlias, leftProjection, leftKeyColumnIds,
                    WorkerCommon.getResultSchema(rightSchema.get(), rightColumnsToRead), rightColAlias, rightProjection, rightKeyColumnIds);
            // build the chain joiner.
            Joiner chainJoiner = BasePartitionedChainJoinWorker.buildChainJoiner(transId, timestamp, threadPool, chainTables, chainJoinInfos,
                    partitionJoiner.getJoinedSchema(), workerMetrics);
            logger.info("chain hash table size: " + chainJoiner.getSmallTableSize() + ", duration (ns): " +
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

            // build the hash table for the left partitioned table
            if (chainJoiner.getSmallTableSize() > 0)
            {
                List<Future> leftFutures = new ArrayList<>(leftPartitioned.size());
                int leftSplitSize = leftPartitioned.size() / leftParallelism;
                if (leftPartitioned.size() % leftParallelism > 0)
                {
                    leftSplitSize++;
                }
                for (int i = 0; i < leftPartitioned.size(); i += leftSplitSize)
                {
                    List<String> parts = new LinkedList<>();
                    for (int j = i; j < i + leftSplitSize && j < leftPartitioned.size(); ++j)
                    {
                        parts.add(leftPartitioned.get(j));
                    }
                    leftFutures.add(threadPool.submit(() -> {
                        try
                        {
                            BasePartitionedJoinWorker.buildHashTable(transId, timestamp, partitionJoiner, parts, leftColumnsToRead,
                                    leftInputStorageInfo.getScheme(), hashValues, numPartition, workerMetrics);
                        } catch (Throwable e)
                        {
                            throw new WorkerException("error during hash table construction", e);
                        }
                    }));
                }
                for (Future future : leftFutures)
                {
                    future.get();
                }
                logger.info("hash table size: " + partitionJoiner.getSmallTableSize() + ", duration (ns): " +
                        (workerMetrics.getInputCostNs() + workerMetrics.getComputeCostNs()));

                // scan the right table and do the join.
                if (partitionJoiner.getSmallTableSize() > 0)
                {
                    int rightSplitSize = rightPartitioned.size() / rightParallelism;
                    if (rightPartitioned.size() % rightParallelism > 0)
                    {
                        rightSplitSize++;
                    }

                    for (int i = 0; i < rightPartitioned.size(); i += rightSplitSize)
                    {
                        List<String> parts = new LinkedList<>();
                        for (int j = i; j < i + rightSplitSize && j < rightPartitioned.size(); ++j)
                        {
                            parts.add(rightPartitioned.get(j));
                        }
                        threadPool.execute(() -> {
                            try
                            {
                                int numJoinedRows = partitionOutput ?
                                        BasePartitionedChainJoinWorker.joinWithRightTableAndPartition(
                                                transId, timestamp, partitionJoiner, chainJoiner, parts, rightColumnsToRead,
                                                rightInputStorageInfo.getScheme(), hashValues, numPartition,
                                                outputPartitionInfo, result, workerMetrics) :
                                        BasePartitionedChainJoinWorker.joinWithRightTable(transId, timestamp, partitionJoiner, chainJoiner, parts, rightColumnsToRead,
                                                rightInputStorageInfo.getScheme(), hashValues, numPartition,
                                                result.get(0), workerMetrics);
                            } catch (Throwable e)
                            {
                                throw new WorkerException("error during hash join", e);
                            }
                        });
                    }
                    threadPool.shutdown();
                    try
                    {
                        while (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) ;
                    } catch (InterruptedException e)
                    {
                        throw new WorkerException("interrupted while waiting for the termination of join", e);
                    }
                }

                String outputPath = outputFolder + outputInfo.getFileNames().get(0);
                try
                {
                    PixelsWriter pixelsWriter;
                    WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                    if (partitionOutput)
                    {
                        pixelsWriter = WorkerCommon.getWriter(chainJoiner.getJoinedSchema(),
                                WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
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
                    } else
                    {
                        pixelsWriter = WorkerCommon.getWriter(chainJoiner.getJoinedSchema(),
                                WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                                encoding, false, null);
                        ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result.get(0);
                        for (VectorizedRowBatch rowBatch : rowBatches)
                        {
                            pixelsWriter.addRowBatch(rowBatch);
                        }
                    }
                    pixelsWriter.close();
                    joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                    if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                    {
                        while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                        {
                            // Wait for 10ms and see if the output file is visible.
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                    workerMetrics.addOutputCostNs(writeCostTimer.stop());
                    workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                    workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                } catch (Throwable e)
                {
                    throw new WorkerException("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
                }
            }

            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(joinOutput, workerMetrics);
            return joinOutput;
        } catch (Throwable e)
        {
            logger.error("error during join", e);
            joinOutput.setSuccessful(false);
            joinOutput.setErrorMessage(e.getMessage());
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return joinOutput;
        }
    }
}
