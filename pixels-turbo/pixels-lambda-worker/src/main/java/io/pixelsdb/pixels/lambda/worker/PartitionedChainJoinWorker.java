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
package io.pixelsdb.pixels.lambda.worker;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.executor.join.Partitioner;
import io.pixelsdb.pixels.turbo.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.turbo.planner.plan.physical.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.turbo.planner.plan.physical.output.JoinOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.Minio.ConfigMinio;
import static io.pixelsdb.pixels.common.physical.storage.Redis.ConfigRedis;
import static io.pixelsdb.pixels.lambda.worker.BroadcastChainJoinWorker.buildFirstJoiner;
import static io.pixelsdb.pixels.lambda.worker.BroadcastChainJoinWorker.chainJoin;
import static java.util.Objects.requireNonNull;

/**
 * Partitioned chain join is the combination of a set of broadcast joins and a partitioned join.
 * It contains a set of chain left tables that are broadcast, a left partitioned table, and a
 * right partitioned table. The join result of the chain tables are then joined with the join
 * result of the left and right partitioned tables.
 *
 * @author hank
 * @date 26/06/2022
 */
public class PartitionedChainJoinWorker implements RequestHandler<PartitionedChainJoinInput, JoinOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionedChainJoinWorker.class);
    private final MetricsCollector metricsCollector = new MetricsCollector();

    @Override
    public JoinOutput handleRequest(PartitionedChainJoinInput event, Context context)
    {
        JoinOutput joinOutput = new JoinOutput();
        long startTime = System.currentTimeMillis();
        joinOutput.setStartTimeMs(startTime);
        joinOutput.setRequestId(context.getAwsRequestId());
        joinOutput.setSuccessful(true);
        joinOutput.setErrorMessage("");
        metricsCollector.clear();

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            // String requestId = context.getAwsRequestId();

            long queryId = event.getQueryId();
            List<BroadcastTableInfo> chainTables = event.getChainTables();
            List<ChainJoinInfo> chainJoinInfos = event.getChainJoinInfos();
            requireNonNull(chainTables, "leftTables is null");
            requireNonNull(chainJoinInfos, "chainJoinInfos is null");
            checkArgument(chainTables.size() == chainJoinInfos.size(),
                    "left table num is not consistent with chain-join info num.");
            checkArgument(chainTables.size() > 1, "there should be at least two chain tables");

            List<String> leftPartitioned = event.getSmallTable().getInputFiles();
            requireNonNull(leftPartitioned, "leftPartitioned is null");
            checkArgument(leftPartitioned.size() > 0, "leftPartitioned is empty");
            int leftParallelism = event.getSmallTable().getParallelism();
            checkArgument(leftParallelism > 0, "leftParallelism is not positive");
            String[] leftCols = event.getSmallTable().getColumnsToRead();
            int[] leftKeyColumnIds = event.getSmallTable().getKeyColumnIds();

            List<String> rightPartitioned = event.getLargeTable().getInputFiles();
            requireNonNull(rightPartitioned, "rightPartitioned is null");
            checkArgument(rightPartitioned.size() > 0, "rightPartitioned is empty");
            int rightParallelism = event.getLargeTable().getParallelism();
            checkArgument(rightParallelism > 0, "rightParallelism is not positive");
            String[] rightCols = event.getLargeTable().getColumnsToRead();
            int[] rightKeyColumnIds = event.getLargeTable().getKeyColumnIds();

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
            StorageInfo storageInfo = outputInfo.getStorageInfo();
            checkArgument(outputInfo.getFileNames().size() == 1,
                    "it is incorrect to have more than one output files");
            String outputFolder = outputInfo.getPath();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = outputInfo.isEncoding();

            ChainJoinInfo lastJoin = chainJoinInfos.get(chainJoinInfos.size()-1);
            boolean partitionOutput = lastJoin.isPostPartition();
            PartitionInfo outputPartitionInfo = lastJoin.getPostPartitionInfo();

            if (partitionOutput)
            {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }

            try
            {
                if (WorkerCommon.minio == null && storageInfo.getScheme() == Storage.Scheme.minio)
                {
                    ConfigMinio(storageInfo.getEndpoint(), storageInfo.getAccessKey(), storageInfo.getSecretKey());
                    WorkerCommon.minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                }
                else if (WorkerCommon.redis == null && storageInfo.getScheme() == Storage.Scheme.redis)
                {
                    ConfigRedis(storageInfo.getEndpoint(), storageInfo.getAccessKey(), storageInfo.getSecretKey());
                    WorkerCommon.redis = StorageFactory.Instance().getStorage(Storage.Scheme.redis);
                }
            } catch (Exception e)
            {
                throw new PixelsWorkerException("failed to initialize Minio storage", e);
            }

            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            WorkerCommon.getFileSchemaFromPaths(threadPool, WorkerCommon.s3, leftSchema, rightSchema, leftPartitioned, rightPartitioned);
            Joiner partitionJoiner = new Joiner(joinType,
                    leftSchema.get(), leftColAlias, leftProjection, leftKeyColumnIds,
                    rightSchema.get(), rightColAlias, rightProjection, rightKeyColumnIds);
            // build the chain joiner.
            Joiner chainJoiner = buildChainJoiner(queryId, threadPool, chainTables, chainJoinInfos,
                    partitionJoiner.getJoinedSchema(), metricsCollector);
            logger.info("chain hash table size: " + chainJoiner.getSmallTableSize() + ", duration (ns): " +
                    (metricsCollector.getInputCostNs() + metricsCollector.getComputeCostNs()));

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

            // build the hash table for the left partitioned table.
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
                            PartitionedJoinWorker.buildHashTable(queryId, partitionJoiner, parts, leftCols, hashValues, numPartition, metricsCollector);
                        } catch (Exception e)
                        {
                            throw new PixelsWorkerException("error during hash table construction", e);
                        }
                    }));
                }
                for (Future future : leftFutures)
                {
                    future.get();
                }
                logger.info("hash table size: " + partitionJoiner.getSmallTableSize() + ", duration (ns): " +
                        (metricsCollector.getInputCostNs() + metricsCollector.getComputeCostNs()));

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
                                        joinWithRightTableAndPartition(
                                                queryId, partitionJoiner, chainJoiner, parts, rightCols, hashValues,
                                                numPartition, outputPartitionInfo, result, metricsCollector) :
                                        joinWithRightTable(queryId, partitionJoiner, chainJoiner, parts, rightCols,
                                                hashValues, numPartition, result.get(0), metricsCollector);
                            } catch (Exception e)
                            {
                                throw new PixelsWorkerException("error during hash join", e);
                            }
                        });
                    }
                    threadPool.shutdown();
                    try
                    {
                        while (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) ;
                    } catch (InterruptedException e)
                    {
                        throw new PixelsWorkerException("interrupted while waiting for the termination of join", e);
                    }
                }
            }

            String outputPath = outputFolder + outputInfo.getFileNames().get(0);
            try
            {
                PixelsWriter pixelsWriter;
                MetricsCollector.Timer writeCostTimer = new MetricsCollector.Timer().start();
                if (partitionOutput)
                {
                    pixelsWriter = WorkerCommon.getWriter(chainJoiner.getJoinedSchema(),
                            WorkerCommon.getStorage(storageInfo.getScheme()), outputPath,
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
                            WorkerCommon.getStorage(storageInfo.getScheme()), outputPath,
                            encoding, false, null);
                    ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result.get(0);
                    for (VectorizedRowBatch rowBatch : rowBatches)
                    {
                        pixelsWriter.addRowBatch(rowBatch);
                    }
                }
                pixelsWriter.close();
                joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                if (storageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!WorkerCommon.minio.exists(outputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }
                metricsCollector.addOutputCostNs(writeCostTimer.stop());
                metricsCollector.addWriteBytes(pixelsWriter.getCompletedBytes());
                metricsCollector.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
            } catch (Exception e)
            {
                throw new PixelsWorkerException("failed to scan the partitioned file '" +
                        rightPartitioned + "' and do the join", e);
            }

            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(joinOutput, metricsCollector);
            return joinOutput;
        } catch (Exception e)
        {
            logger.error("error during join", e);
            joinOutput.setSuccessful(false);
            joinOutput.setErrorMessage(e.getMessage());
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return joinOutput;
        }
    }

    private static Joiner buildChainJoiner(
            long queryId, ExecutorService executor, List<BroadcastTableInfo> chainTables,
            List<ChainJoinInfo> chainJoinInfos, TypeDescription lastResultSchema, MetricsCollector metricsCollector)
    {
        requireNonNull(executor, "executor is null");
        requireNonNull(chainTables, "chainTables is null");
        requireNonNull(chainJoinInfos, "chainJoinInfos is null");
        checkArgument(chainTables.size() == chainJoinInfos.size() && chainTables.size() > 1,
                "the size of chainTables and chainJoinInfos must be the same, and larger than 1");
        try
        {
            BroadcastTableInfo t1 = chainTables.get(0);
            BroadcastTableInfo t2 = chainTables.get(1);
            ChainJoinInfo currChainJoin = chainJoinInfos.get(0);
            MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
            Joiner currJoiner = buildFirstJoiner(queryId, executor, t1, t2, currChainJoin, metricsCollector);
            for (int i = 1; i < chainTables.size() - 1; ++i)
            {
                BroadcastTableInfo currRightTable = chainTables.get(i);
                BroadcastTableInfo nextChainTable = chainTables.get(i+1);
                readCostTimer.start();
                TypeDescription nextTableSchema = WorkerCommon.getFileSchemaFromSplits(WorkerCommon.s3, nextChainTable.getInputSplits());
                TypeDescription nextResultSchema = WorkerCommon.getResultSchema(
                        nextTableSchema, nextChainTable.getColumnsToRead());
                readCostTimer.stop();

                ChainJoinInfo nextChainJoin = chainJoinInfos.get(i);
                Joiner nextJoiner = new Joiner(nextChainJoin.getJoinType(),
                        currJoiner.getJoinedSchema(), nextChainJoin.getSmallColumnAlias(),
                        nextChainJoin.getSmallProjection(), currChainJoin.getKeyColumnIds(),
                        nextResultSchema, nextChainJoin.getLargeColumnAlias(),
                        nextChainJoin.getLargeProjection(), nextChainTable.getKeyColumnIds());

                chainJoin(queryId, executor, currJoiner, nextJoiner, currRightTable, metricsCollector);
                currJoiner = nextJoiner;
                currChainJoin = nextChainJoin;
            }
            BroadcastTableInfo lastChainTable = chainTables.get(chainTables.size()-1);
            ChainJoinInfo lastChainJoin = chainJoinInfos.get(chainJoinInfos.size()-1);
            Joiner finalJoiner = new Joiner(lastChainJoin.getJoinType(),
                    currJoiner.getJoinedSchema(), lastChainJoin.getSmallColumnAlias(),
                    lastChainJoin.getSmallProjection(), currChainJoin.getKeyColumnIds(),
                    lastResultSchema, lastChainJoin.getLargeColumnAlias(),
                    lastChainJoin.getLargeProjection(), lastChainJoin.getKeyColumnIds());
            chainJoin(queryId, executor, currJoiner, finalJoiner, lastChainTable, metricsCollector);
            metricsCollector.addInputCostNs(readCostTimer.getElapsedNs());
            return finalJoiner;
        } catch (Exception e)
        {
            throw new RuntimeException("failed to join left tables", e);
        }
    }

    /**
     * Scan the partitioned file of the right table and do the join.
     *
     * @param queryId the query id used by I/O scheduler
     * @param partitionedJoiner the joiner for the partitioned join
     * @param chainJoiner the joiner of the final chain join
     * @param rightParts the information of partitioned files of the right table
     * @param rightCols the column names of the right table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param joinResult the container of the join result
     * @param metricsCollector the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    protected static int joinWithRightTable(
            long queryId, Joiner partitionedJoiner, Joiner chainJoiner, List<String> rightParts, String[] rightCols,
            List<Integer> hashValues, int numPartition, ConcurrentLinkedQueue<VectorizedRowBatch> joinResult,
            MetricsCollector metricsCollector)
    {
        int joinedRows = 0;
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeCostTimer = new MetricsCollector.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(rightPartitioned, WorkerCommon.s3))
                {
                    readCostTimer.stop();
                    checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                    Set<Integer> rightHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                    for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                    {
                        rightHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                    }
                    for (int hashValue : hashValues)
                    {
                        if (!rightHashValues.contains(hashValue))
                        {
                            continue;
                        }
                        PixelsReaderOption option = WorkerCommon.getReaderOption(queryId, rightCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rightRowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");

                        computeCostTimer.start();
                        do
                        {
                            rightRowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                            if (rightRowBatch.size > 0)
                            {
                                List<VectorizedRowBatch> partitionedJoinResults =
                                        partitionedJoiner.join(rightRowBatch);
                                for (VectorizedRowBatch partitionedJoinResult : partitionedJoinResults)
                                {
                                    if (!partitionedJoinResult.isEmpty())
                                    {
                                        List<VectorizedRowBatch> chainJoinResults =
                                                chainJoiner.join(partitionedJoinResult);
                                        for (VectorizedRowBatch chainJoinResult : chainJoinResults)
                                        {
                                            if (!chainJoinResult.isEmpty())
                                            {
                                                joinResult.add(chainJoinResult);
                                                joinedRows += chainJoinResult.size;
                                            }
                                        }
                                    }
                                }
                            }
                        } while (!rightRowBatch.endOfFile);
                        computeCostTimer.stop();
                        computeCostTimer.minus(recordReader.getReadTimeNanos());
                        readCostTimer.add(recordReader.getReadTimeNanos());
                        readBytes += recordReader.getCompletedBytes();
                        numReadRequests += recordReader.getNumReadRequests();
                    }
                    it.remove();
                } catch (Exception e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new PixelsWorkerException("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
                }
            }
            if (!rightParts.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new PixelsWorkerException("interrupted while waiting for the partitioned files");
                }
            }
        }
        metricsCollector.addReadBytes(readBytes);
        metricsCollector.addNumReadRequests(numReadRequests);
        metricsCollector.addInputCostNs(readCostTimer.getElapsedNs());
        metricsCollector.addComputeCostNs(computeCostTimer.getElapsedNs());
        return joinedRows;
    }

    /**
     * Scan the partitioned file of the right table, do the join, and partition the output.
     *
     * @param queryId the query id used by I/O scheduler
     * @param partitionedJoiner the joiner for the partitioned join
     * @param chainJoiner the joiner for the final chain join
     * @param rightParts the information of partitioned files of the right table
     * @param rightCols the column names of the right table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param postPartitionInfo the partition information of post partitioning
     * @param partitionResult the container of the join and post partitioning result
     * @param metricsCollector the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    protected static int joinWithRightTableAndPartition(
            long queryId, Joiner partitionedJoiner, Joiner chainJoiner, List<String> rightParts, String[] rightCols,
            List<Integer> hashValues, int numPartition, PartitionInfo postPartitionInfo,
            List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult, MetricsCollector metricsCollector)
    {
        requireNonNull(postPartitionInfo, "outputPartitionInfo is null");
        Partitioner partitioner = new Partitioner(postPartitionInfo.getNumPartition(),
                WorkerCommon.rowBatchSize, chainJoiner.getJoinedSchema(), postPartitionInfo.getKeyColumnIds());
        int joinedRows = 0;
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeCostTimer = new MetricsCollector.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(rightPartitioned, WorkerCommon.s3))
                {
                    readCostTimer.stop();
                    checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                    Set<Integer> rightHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                    for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                    {
                        rightHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                    }
                    for (int hashValue : hashValues)
                    {
                        if (!rightHashValues.contains(hashValue))
                        {
                            continue;
                        }
                        PixelsReaderOption option = WorkerCommon.getReaderOption(queryId, rightCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rightBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");

                        computeCostTimer.start();
                        do
                        {
                            rightBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                            if (rightBatch.size > 0)
                            {
                                List<VectorizedRowBatch> partitionedJoinResults = partitionedJoiner.join(rightBatch);
                                for (VectorizedRowBatch partitionedJoinResult : partitionedJoinResults)
                                {
                                    if (!partitionedJoinResult.isEmpty())
                                    {
                                        List<VectorizedRowBatch> chainJoinResults =
                                                chainJoiner.join(partitionedJoinResult);
                                        for (VectorizedRowBatch chainJoinResult : chainJoinResults)
                                        {
                                            if (!chainJoinResult.isEmpty())
                                            {
                                                Map<Integer, VectorizedRowBatch> parts =
                                                        partitioner.partition(chainJoinResult);
                                                for (Map.Entry<Integer, VectorizedRowBatch> entry : parts.entrySet())
                                                {
                                                    partitionResult.get(entry.getKey()).add(entry.getValue());
                                                }
                                                joinedRows += chainJoinResult.size;
                                            }
                                        }

                                    }
                                }
                            }
                        } while (!rightBatch.endOfFile);
                        computeCostTimer.stop();
                        computeCostTimer.minus(recordReader.getReadTimeNanos());
                        readCostTimer.add(recordReader.getReadTimeNanos());
                        readBytes += recordReader.getCompletedBytes();
                        numReadRequests += recordReader.getNumReadRequests();
                    }
                    it.remove();
                } catch (Exception e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new PixelsWorkerException("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
                }
            }
            if (!rightParts.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new PixelsWorkerException("interrupted while waiting for the partitioned files");
                }
            }
        }

        VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
        for (int hash = 0; hash < tailBatches.length; ++hash)
        {
            if (!tailBatches[hash].isEmpty())
            {
                partitionResult.get(hash).add(tailBatches[hash]);
            }
        }
        metricsCollector.addReadBytes(readBytes);
        metricsCollector.addNumReadRequests(numReadRequests);
        metricsCollector.addInputCostNs(readCostTimer.getElapsedNs());
        metricsCollector.addComputeCostNs(computeCostTimer.getElapsedNs());
        return joinedRows;
    }
}
