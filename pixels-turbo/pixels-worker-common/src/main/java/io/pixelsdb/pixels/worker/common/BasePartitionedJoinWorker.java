/*
 * Copyright 2022-2023 PixelsDB.
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
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2022-05-07
 * @update 2023-04-23 (moved from pixels-worker-lambda to here as the base worker implementation)
 */
public class BasePartitionedJoinWorker extends Worker<PartitionedJoinInput, JoinOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BasePartitionedJoinWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    @Override
    public JoinOutput process(PartitionedJoinInput event)
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
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            long timestamp = event.getTimestamp();
            requireNonNull(event.getSmallTable(), "event.smallTable is null");
            StorageInfo leftInputStorageInfo = event.getSmallTable().getStorageInfo();
            List<String> leftPartitioned = event.getSmallTable().getInputFiles();
            requireNonNull(leftPartitioned, "leftPartitioned is null");
            checkArgument(leftPartitioned.size() > 0, "leftPartitioned is empty");
            int leftParallelism = event.getSmallTable().getParallelism();
            checkArgument(leftParallelism > 0, "leftParallelism is not positive");
            String[] leftColumnsToRead = event.getSmallTable().getColumnsToRead();
            int[] leftKeyColumnIds = event.getSmallTable().getKeyColumnIds();

            requireNonNull(event.getLargeTable(), "event.largeTable is null");
            StorageInfo rightInputStorageInfo = event.getLargeTable().getStorageInfo();
            List<String> rightPartitioned = event.getLargeTable().getInputFiles();
            requireNonNull(rightPartitioned, "rightPartitioned is null");
            checkArgument(rightPartitioned.size() > 0, "rightPartitioned is empty");
            int rightParallelism = event.getLargeTable().getParallelism();
            checkArgument(rightParallelism > 0, "rightParallelism is not positive");
            String[] rightColumnsToRead = event.getLargeTable().getColumnsToRead();
            int[] rightKeyColumnIds = event.getLargeTable().getKeyColumnIds();

            String[] leftColAlias = event.getJoinInfo().getSmallColumnAlias();
            String[] rightColAlias = event.getJoinInfo().getLargeColumnAlias();
            boolean[] leftProjection = event.getJoinInfo().getSmallProjection();
            boolean[] rightProjection = event.getJoinInfo().getLargeProjection();
            JoinType joinType = event.getJoinInfo().getJoinType();
            List<Integer> hashValues = event.getJoinInfo().getHashValues();
            int numPartition = event.getJoinInfo().getNumPartition();
            logger.info("small table: " + event.getSmallTable().getTableName() +
                    ", large table: " + event.getLargeTable().getTableName() +
                    ", number of partitions (" + numPartition + ")");

            MultiOutputInfo outputInfo = event.getOutput();
            StorageInfo outputStorageInfo = outputInfo.getStorageInfo();
            if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
            {
                checkArgument(outputInfo.getFileNames().size() == 2,
                        "it is incorrect to have more than two output files");
            }
            else
            {
                checkArgument(outputInfo.getFileNames().size() == 1,
                        "it is incorrect to have more than one output files");
            }
            String outputFolder = outputInfo.getPath();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = outputInfo.isEncoding();

            boolean partitionOutput = event.getJoinInfo().isPostPartition();
            PartitionInfo outputPartitionInfo = event.getJoinInfo().getPostPartitionInfo();
            if (partitionOutput)
            {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }

            WorkerCommon.initStorage(leftInputStorageInfo);
            WorkerCommon.initStorage(rightInputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            // build the joiner.
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
            Joiner joiner = new Joiner(joinType,
                    WorkerCommon.getResultSchema(leftSchema.get(), leftColumnsToRead),
                    leftColAlias, leftProjection, leftKeyColumnIds,
                    WorkerCommon.getResultSchema(rightSchema.get(), rightColumnsToRead),
                    rightColAlias, rightProjection, rightKeyColumnIds);
            // build the hash table for the left table.
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
                        buildHashTable(transId, timestamp, joiner, parts, leftColumnsToRead, leftInputStorageInfo.getScheme(),
                                hashValues, numPartition, workerMetrics);
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

            // scan the right table and do the join.
            if (joiner.getSmallTableSize() > 0)
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
                                            transId, timestamp, joiner, parts, rightColumnsToRead,
                                            rightInputStorageInfo.getScheme(), hashValues,
                                            numPartition, outputPartitionInfo, result, workerMetrics) :
                                    joinWithRightTable(transId, timestamp, joiner, parts, rightColumnsToRead,
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

                if (exceptionHandler.hasException())
                {
                    throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
                }
            }

            String outputPath = outputFolder + outputInfo.getFileNames().get(0);
            try
            {
                WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                PixelsWriter pixelsWriter;
                if (partitionOutput)
                {
                    pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
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
                }
                else
                {
                    pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                            WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                            encoding, false, null);
                    ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result.get(0);
                    for (VectorizedRowBatch rowBatch : rowBatches)
                    {
                        pixelsWriter.addRowBatch(rowBatch);
                    }
                }
                pixelsWriter.close();
                workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }

                if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
                {
                    // output the left-outer tail.
                    outputPath = outputFolder + outputInfo.getFileNames().get(1);
                    if (partitionOutput)
                    {
                        requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
                        pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                                WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                                encoding, true, Arrays.stream(
                                        outputPartitionInfo.getKeyColumnIds()).boxed().
                                        collect(Collectors.toList()));
                        joiner.writeLeftOuterAndPartition(pixelsWriter, WorkerCommon.rowBatchSize,
                                outputPartitionInfo.getNumPartition(), outputPartitionInfo.getKeyColumnIds());
                    }
                    else
                    {
                        pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                                WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                                encoding, false, null);
                        joiner.writeLeftOuter(pixelsWriter, WorkerCommon.rowBatchSize);
                    }
                    pixelsWriter.close();
                    workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                    workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                    joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                    if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                    {
                        while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                        {
                            // Wait for 10ms and see if the output file is visible.
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                }
                workerMetrics.addOutputCostNs(writeCostTimer.stop());
            } catch (Throwable e)
            {
                throw new WorkerException(
                        "failed to finish writing and close the join result file '" + outputPath + "'", e);
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

    /**
     * Scan the partitioned file of the left table and populate the hash table for the join.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param timestamp the transaction timestamp
     * @param joiner the joiner for which the hash table is built
     * @param leftParts the information of partitioned files of the left table
     * @param leftCols the column names of the left table
     * @param leftScheme the storage scheme of the left table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param workerMetrics the collector of the performance metrics
     */
    protected static void buildHashTable(long transId, long timestamp, Joiner joiner, List<String> leftParts, String[] leftCols,
                                         Storage.Scheme leftScheme, List<Integer> hashValues, int numPartition,
                                         WorkerMetrics workerMetrics)
    {
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!leftParts.isEmpty())
        {
            for (Iterator<String> it = leftParts.iterator(); it.hasNext(); )
            {
                String leftPartitioned = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        leftPartitioned, WorkerCommon.getStorage(leftScheme)))
                {
                    readCostTimer.stop();
                    checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                    Set<Integer> leftHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                    for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                    {
                        leftHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                    }
                    for (int hashValue : hashValues)
                    {
                        if (!leftHashValues.contains(hashValue))
                        {
                            continue;
                        }
                        PixelsReaderOption option = WorkerCommon.getReaderOption(transId, timestamp, leftCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");

                        computeCostTimer.start();
                        do
                        {
                            rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                            if (rowBatch.size > 0)
                            {
                                joiner.populateLeftTable(rowBatch);
                            }
                        } while (!rowBatch.endOfFile);
                        computeCostTimer.stop();
                        computeCostTimer.minus(recordReader.getReadTimeNanos());
                        readCostTimer.add(recordReader.getReadTimeNanos());
                        readBytes += recordReader.getCompletedBytes();
                        numReadRequests += recordReader.getNumReadRequests();
                    }
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the partitioned file '" +
                            leftPartitioned + "' and build the hash table", e);
                }
            }
            if (!leftParts.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the partitioned files");
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
    }

    /**
     * Scan the partitioned file of the right table and do the join.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param timestamp the transaction timestamp
     * @param joiner the joiner for the partitioned join
     * @param rightParts the information of partitioned files of the right table
     * @param rightCols the column names of the right table
     * @param rightScheme the storage scheme of the right table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param joinResult the container of the join result
     * @param workerMetrics the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    protected static int joinWithRightTable(
            long transId, long timestamp, Joiner joiner, List<String> rightParts, String[] rightCols, Storage.Scheme rightScheme,
            List<Integer> hashValues, int numPartition, ConcurrentLinkedQueue<VectorizedRowBatch> joinResult,
            WorkerMetrics workerMetrics)
    {
        int joinedRows = 0;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        rightPartitioned, WorkerCommon.getStorage(rightScheme)))
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
                        PixelsReaderOption option = WorkerCommon.getReaderOption(transId, timestamp, rightCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");

                        computeCostTimer.start();
                        do
                        {
                            rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                            if (rowBatch.size > 0)
                            {
                                List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                                for (VectorizedRowBatch joined : joinedBatches)
                                {
                                    if (!joined.isEmpty())
                                    {
                                        joinResult.add(joined);
                                        joinedRows += joined.size;
                                    }
                                }
                            }
                        } while (!rowBatch.endOfFile);
                        computeCostTimer.stop();
                        computeCostTimer.minus(recordReader.getReadTimeNanos());
                        readCostTimer.add(recordReader.getReadTimeNanos());
                        readBytes += recordReader.getCompletedBytes();
                        numReadRequests += recordReader.getNumReadRequests();
                    }
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the partitioned file '" +
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
                    throw new WorkerException("interrupted while waiting for the partitioned files");
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        return joinedRows;
    }

    /**
     * Scan the partitioned file of the right table, do the join, and partition the output.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param timestamp the transaction timestamp
     * @param joiner the joiner for the partitioned join
     * @param rightParts the information of partitioned files of the right table
     * @param rightCols the column names of the right table
     * @param rightScheme the storage scheme of the right table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param postPartitionInfo the partition information of post partitioning
     * @param partitionResult the container of the join and post partitioning result
     * @param workerMetrics the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    protected static int joinWithRightTableAndPartition(
            long transId, long timestamp, Joiner joiner, List<String> rightParts, String[] rightCols, Storage.Scheme rightScheme,
            List<Integer> hashValues, int numPartition, PartitionInfo postPartitionInfo,
            List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult, WorkerMetrics workerMetrics)
    {
        requireNonNull(postPartitionInfo, "outputPartitionInfo is null");
        Partitioner partitioner = new Partitioner(postPartitionInfo.getNumPartition(),
                WorkerCommon.rowBatchSize, joiner.getJoinedSchema(), postPartitionInfo.getKeyColumnIds());
        int joinedRows = 0;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        rightPartitioned, WorkerCommon.getStorage(rightScheme)))
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
                        PixelsReaderOption option = WorkerCommon.getReaderOption(transId, timestamp, rightCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");

                        computeCostTimer.start();
                        do
                        {
                            rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                            if (rowBatch.size > 0)
                            {
                                List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                                for (VectorizedRowBatch joined : joinedBatches)
                                {
                                    if (!joined.isEmpty())
                                    {
                                        Map<Integer, VectorizedRowBatch> parts = partitioner.partition(joined);
                                        for (Map.Entry<Integer, VectorizedRowBatch> entry : parts.entrySet())
                                        {
                                            partitionResult.get(entry.getKey()).add(entry.getValue());
                                        }
                                        joinedRows += joined.size;
                                    }
                                }
                            }
                        } while (!rowBatch.endOfFile);
                        computeCostTimer.stop();
                        computeCostTimer.minus(recordReader.getReadTimeNanos());
                        readCostTimer.add(recordReader.getReadTimeNanos());
                        readBytes += recordReader.getCompletedBytes();
                        numReadRequests += recordReader.getNumReadRequests();
                    }
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the partitioned file '" +
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
                    throw new WorkerException("interrupted while waiting for the partitioned files");
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
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        return joinedRows;
    }
}
