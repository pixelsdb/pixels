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
import io.pixelsdb.pixels.executor.join.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import io.pixelsdb.pixels.storage.s3qs.S3QueueMessage;
import io.pixelsdb.pixels.storage.s3qs.S3QueuePollResult;
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
            boolean leftS3QS = isS3QSShuffle(event.getSmallTable());
            StorageInfo leftInputStorageInfo = event.getSmallTable().getStorageInfo();
            List<String> leftPartitioned = event.getSmallTable().getInputFiles();
            if (!leftS3QS)
            {
                requireNonNull(leftPartitioned, "leftPartitioned is null");
                checkArgument(leftPartitioned.size() > 0, "leftPartitioned is empty");
            }
            int leftParallelism = event.getSmallTable().getParallelism();
            checkArgument(leftParallelism > 0, "leftParallelism is not positive");
            String[] leftColumnsToRead = event.getSmallTable().getColumnsToRead();
            int[] leftKeyColumnIds = event.getSmallTable().getKeyColumnIds();

            requireNonNull(event.getLargeTable(), "event.largeTable is null");
            boolean rightS3QS = isS3QSShuffle(event.getLargeTable());
            StorageInfo rightInputStorageInfo = event.getLargeTable().getStorageInfo();
            List<String> rightPartitioned = event.getLargeTable().getInputFiles();
            if (!rightS3QS)
            {
                requireNonNull(rightPartitioned, "rightPartitioned is null");
                checkArgument(rightPartitioned.size() > 0, "rightPartitioned is empty");
            }
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
            WorkerCommon.initOptionalShuffleStorage(event.getSmallTable().getShuffleInfo());
            WorkerCommon.initOptionalShuffleStorage(event.getLargeTable().getShuffleInfo());
            WorkerCommon.initStorage(outputStorageInfo);

            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            Map<Integer, Queue<S3QueuePollResult>> leftPendingMessages = new HashMap<>();
            Map<Integer, Queue<S3QueuePollResult>> rightPendingMessages = new HashMap<>();
            if (!leftS3QS && !rightS3QS)
            {
                WorkerCommon.getFileSchemaFromPaths(threadPool,
                        WorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                        WorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                        leftSchema, rightSchema, leftPartitioned, rightPartitioned);
            }
            else
            {
                if (leftS3QS)
                {
                    leftSchema.set(getS3QSFileSchema(
                            event.getSmallTable().getShuffleInfo(), hashValues, leftPendingMessages));
                }
                else
                {
                    leftSchema.set(WorkerCommon.getFileSchemaFromPaths(
                            WorkerCommon.getStorage(leftInputStorageInfo.getScheme()), leftPartitioned));
                }
                if (rightS3QS)
                {
                    rightSchema.set(getS3QSFileSchema(
                            event.getLargeTable().getShuffleInfo(), hashValues, rightPendingMessages));
                }
                else
                {
                    rightSchema.set(WorkerCommon.getFileSchemaFromPaths(
                            WorkerCommon.getStorage(rightInputStorageInfo.getScheme()), rightPartitioned));
                }
            }
            /*
             * Issue #450:
             * For the left and the right partial partitioned files, the file schema is equal to the columns to read in normal cases.
             * However, it is safer to turn file schema into result schema here.
             */
            Joiner joiner = new HashJoiner(joinType,
                    WorkerCommon.getResultSchema(leftSchema.get(), leftColumnsToRead),
                    leftColAlias, leftProjection, leftKeyColumnIds,
                    WorkerCommon.getResultSchema(rightSchema.get(), rightColumnsToRead),
                    rightColAlias, rightProjection, rightKeyColumnIds);
            // build the hash table for the left table.
            if (leftS3QS)
            {
                buildHashTableS3QS(transId, timestamp, (HashJoiner) joiner, event.getSmallTable().getShuffleInfo(),
                        leftPendingMessages, leftColumnsToRead, hashValues, workerMetrics);
            }
            else
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
                            buildHashTable(transId, timestamp, (HashJoiner) joiner, parts, leftColumnsToRead, leftInputStorageInfo.getScheme(),
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
                if (rightS3QS)
                {
                    if (partitionOutput)
                    {
                        joinWithRightTableAndPartitionS3QS(transId, timestamp, joiner,
                                event.getLargeTable().getShuffleInfo(), rightPendingMessages, rightColumnsToRead,
                                hashValues, outputPartitionInfo, result, workerMetrics);
                    }
                    else
                    {
                        joinWithRightTableS3QS(transId, timestamp, joiner, event.getLargeTable().getShuffleInfo(),
                                rightPendingMessages, rightColumnsToRead, hashValues, result.get(0), workerMetrics);
                    }
                }
                else
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
            }
            else if (rightS3QS)
            {
                for (int hashValue : hashValues)
                {
                    drainS3QSPartition(event.getLargeTable().getShuffleInfo(), hashValue, rightPendingMessages,
                            message -> { });
                }
            }
            if (!threadPool.isShutdown())
            {
                threadPool.shutdown();
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
                        ((HashJoiner)joiner).writeLeftOuterAndPartition(pixelsWriter, WorkerCommon.rowBatchSize,
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

    protected interface S3QSDataHandler
    {
        void handle(S3QueueMessage message) throws Exception;
    }

    protected interface RowBatchHandler
    {
        void handle(VectorizedRowBatch rowBatch) throws Exception;
    }

    protected static boolean isS3QSShuffle(PartitionedTableInfo tableInfo)
    {
        ShuffleInfo shuffleInfo = tableInfo.getShuffleInfo();
        return shuffleInfo != null &&
                shuffleInfo.getStorageInfo() != null &&
                shuffleInfo.getStorageInfo().getScheme() == Storage.Scheme.s3qs;
    }

    private static S3QS getS3QSStorage()
    {
        Storage storage = WorkerCommon.getStorage(Storage.Scheme.s3qs);
        if (!(storage instanceof S3QS))
        {
            throw new WorkerException("storage of scheme s3qs is not S3QS");
        }
        return (S3QS) storage;
    }

    protected static TypeDescription getS3QSFileSchema(ShuffleInfo shuffleInfo, List<Integer> hashValues,
                                                       Map<Integer, Queue<S3QueuePollResult>> pendingMessages)
            throws IOException
    {
        checkArgument(!hashValues.isEmpty(), "hashValues is empty");
        S3QS s3qs = getS3QSStorage();
        int partitionId = hashValues.get(0);
        while (true)
        {
            S3QueuePollResult result = pollS3QSMessage(s3qs, shuffleInfo, partitionId, pendingMessages);
            if (result == null)
            {
                continue;
            }
            addPendingMessage(pendingMessages, partitionId, result);
            S3QueueMessage message = result.getMessage();
            if (message.getMetadata() != null && !message.getMetadata().isEmpty())
            {
                return TypeDescription.fromString(message.getMetadata());
            }
            if (message.isData())
            {
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        message.getObjectPath(), WorkerCommon.getStorage(Storage.Scheme.s3qs)))
                {
                    return pixelsReader.getFileSchema();
                }
            }
        }
    }

    private static S3QueuePollResult pollS3QSMessage(S3QS s3qs, ShuffleInfo shuffleInfo, int partitionId,
                                                     Map<Integer, Queue<S3QueuePollResult>> pendingMessages)
            throws IOException
    {
        Queue<S3QueuePollResult> pending = pendingMessages.get(partitionId);
        if (pending != null)
        {
            S3QueuePollResult result = pending.poll();
            if (result != null)
            {
                return result;
            }
        }
        S3QueueMessage pollRequest = new S3QueueMessage().setPartitionNum(partitionId);
        S3QueuePollResult result = s3qs.pollMessage(pollRequest, shuffleInfo.getPollTimeoutSeconds());
        if (result != null)
        {
            validateS3QSMessage(shuffleInfo, partitionId, result.getMessage());
        }
        return result;
    }

    private static void addPendingMessage(Map<Integer, Queue<S3QueuePollResult>> pendingMessages, int partitionId,
                                          S3QueuePollResult result)
    {
        Queue<S3QueuePollResult> pending = pendingMessages.get(partitionId);
        if (pending == null)
        {
            pending = new LinkedList<>();
            pendingMessages.put(partitionId, pending);
        }
        pending.add(result);
    }

    private static void validateS3QSMessage(ShuffleInfo shuffleInfo, int partitionId, S3QueueMessage message)
    {
        if (!shuffleInfo.getShuffleId().equals(message.getShuffleId()))
        {
            throw new WorkerException("unexpected s3qs shuffle id: " + message.getShuffleId());
        }
        if (message.getPartitionId() != partitionId)
        {
            throw new WorkerException("unexpected s3qs partition id: " + message.getPartitionId() +
                    ", expected " + partitionId);
        }
    }

    /**
     * Drain one S3QS partition queue until all producer end markers are seen and
     * one final long poll returns empty. DATA messages are acknowledged only
     * after the handler has processed the referenced S3 object.
     */
    protected static void drainS3QSPartition(ShuffleInfo shuffleInfo, int partitionId,
                                             Map<Integer, Queue<S3QueuePollResult>> pendingMessages,
                                             S3QSDataHandler dataHandler) throws Exception
    {
        S3QS s3qs = getS3QSStorage();
        Set<Integer> endedProducers = new HashSet<>(shuffleInfo.getProducerCount());
        while (true)
        {
            S3QueuePollResult result = pollS3QSMessage(s3qs, shuffleInfo, partitionId, pendingMessages);
            if (result == null)
            {
                if (endedProducers.size() >= shuffleInfo.getProducerCount())
                {
                    return;
                }
                continue;
            }

            S3QueueMessage message = result.getMessage();
            message.setReceiptHandle(result.getReceiptHandle());
            if (message.isProducerEnd())
            {
                endedProducers.add(message.getProducerId());
                s3qs.finishWork(message);
            }
            else if (message.isData())
            {
                dataHandler.handle(message);
                s3qs.finishWork(message);
            }
            else
            {
                throw new WorkerException("unsupported s3qs message type: " + message.getMessageType());
            }
        }
    }

    protected static void readS3QSDataObject(long transId, long timestamp, S3QueueMessage message,
                                             String[] columnsToRead, WorkerMetrics workerMetrics,
                                             RowBatchHandler rowBatchHandler)
            throws Exception
    {
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        readCostTimer.start();
        try (PixelsReader pixelsReader = WorkerCommon.getReader(
                message.getObjectPath(), WorkerCommon.getStorage(Storage.Scheme.s3qs)))
        {
            readCostTimer.stop();
            PixelsReaderOption option = WorkerCommon.getReaderOption(
                    transId, timestamp, columnsToRead, new InputInfo(message.getObjectPath(), 0, -1));
            PixelsRecordReader recordReader = pixelsReader.read(option);
            checkArgument(recordReader.isValid(), "failed to get record reader");

            VectorizedRowBatch rowBatch;
            computeCostTimer.start();
            do
            {
                rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                if (rowBatch.size > 0)
                {
                    rowBatchHandler.handle(rowBatch);
                }
            } while (!rowBatch.endOfFile);
            computeCostTimer.stop();
            computeCostTimer.minus(recordReader.getReadTimeNanos());
            readCostTimer.add(recordReader.getReadTimeNanos());
            readBytes += recordReader.getCompletedBytes();
            numReadRequests += recordReader.getNumReadRequests();
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
    }

    protected static void buildHashTableS3QS(long transId, long timestamp, HashJoiner joiner, ShuffleInfo shuffleInfo,
                                             Map<Integer, Queue<S3QueuePollResult>> pendingMessages,
                                             String[] leftCols, List<Integer> hashValues,
                                             WorkerMetrics workerMetrics) throws Exception
    {
        for (int partitionId : hashValues)
        {
            drainS3QSPartition(shuffleInfo, partitionId, pendingMessages, message ->
                    readS3QSDataObject(transId, timestamp, message, leftCols, workerMetrics,
                            joiner::populateLeftTable));
        }
    }

    protected static int joinWithRightTableS3QS(
            long transId, long timestamp, Joiner joiner, ShuffleInfo shuffleInfo,
            Map<Integer, Queue<S3QueuePollResult>> pendingMessages, String[] rightCols,
            List<Integer> hashValues, ConcurrentLinkedQueue<VectorizedRowBatch> joinResult,
            WorkerMetrics workerMetrics) throws Exception
    {
        final int[] joinedRows = {0};
        for (int partitionId : hashValues)
        {
            drainS3QSPartition(shuffleInfo, partitionId, pendingMessages, message ->
                    readS3QSDataObject(transId, timestamp, message, rightCols, workerMetrics, rowBatch -> {
                        List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                        for (VectorizedRowBatch joined : joinedBatches)
                        {
                            if (!joined.isEmpty())
                            {
                                joinResult.add(joined);
                                joinedRows[0] += joined.size;
                            }
                        }
                    }));
        }
        return joinedRows[0];
    }

    protected static int joinWithRightTableAndPartitionS3QS(
            long transId, long timestamp, Joiner joiner, ShuffleInfo shuffleInfo,
            Map<Integer, Queue<S3QueuePollResult>> pendingMessages, String[] rightCols,
            List<Integer> hashValues, PartitionInfo postPartitionInfo,
            List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult, WorkerMetrics workerMetrics)
            throws Exception
    {
        requireNonNull(postPartitionInfo, "outputPartitionInfo is null");
        Partitioner partitioner = new Partitioner(postPartitionInfo.getNumPartition(),
                WorkerCommon.rowBatchSize, joiner.getJoinedSchema(), postPartitionInfo.getKeyColumnIds());
        final int[] joinedRows = {0};
        for (int partitionId : hashValues)
        {
            drainS3QSPartition(shuffleInfo, partitionId, pendingMessages, message ->
                    readS3QSDataObject(transId, timestamp, message, rightCols, workerMetrics, rowBatch -> {
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
                                joinedRows[0] += joined.size;
                            }
                        }
                    }));
        }

        VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
        for (int hash = 0; hash < tailBatches.length; ++hash)
        {
            if (!tailBatches[hash].isEmpty())
            {
                partitionResult.get(hash).add(tailBatches[hash]);
            }
        }
        return joinedRows[0];
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

    protected static void buildHashTable(long transId, long timestamp, HashJoiner joiner, List<String> leftParts, String[] leftCols,
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
                    Set<Integer> leftHashValues;
                    if (leftScheme.equals(Storage.Scheme.httpstream))
                    {
                        leftHashValues = new HashSet<>(hashValues);
                    } else
                    {
                        checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                        leftHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                        for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                        {
                            leftHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                        }
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
                    Set<Integer> rightHashValues;
                    if (rightScheme.equals(Storage.Scheme.httpstream))
                    {
                        rightHashValues = new HashSet<>(hashValues);
                    } else
                    {
                        checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                        rightHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                        for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                        {
                            rightHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                        }
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
                        e.printStackTrace();
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
                    Set<Integer> rightHashValues;
                    if (rightScheme.equals(Storage.Scheme.httpstream))
                    {
                        rightHashValues = new HashSet<>(hashValues);
                    } else
                    {
                        checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                        rightHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                        for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                        {
                            rightHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                        }
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
