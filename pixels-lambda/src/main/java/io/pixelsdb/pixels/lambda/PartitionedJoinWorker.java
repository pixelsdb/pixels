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
package io.pixelsdb.pixels.lambda;

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
import io.pixelsdb.pixels.executor.lambda.domain.MultiOutputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.PartitionInfo;
import io.pixelsdb.pixels.executor.lambda.domain.StorageInfo;
import io.pixelsdb.pixels.executor.lambda.input.PartitionedJoinInput;
import io.pixelsdb.pixels.executor.lambda.output.JoinOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 07/05/2022
 */
public class PartitionedJoinWorker implements RequestHandler<PartitionedJoinInput, JoinOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionedJoinWorker.class);
    private final MetricsCollector metricsCollector = new MetricsCollector();

    @Override
    public JoinOutput handleRequest(PartitionedJoinInput event, Context context)
    {
        existFiles.clear();
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
            List<Integer> hashValues = event.getJoinInfo().getHashValues();
            int numPartition = event.getJoinInfo().getNumPartition();
            logger.info("small table: " + event.getSmallTable().getTableName() +
                    ", large table: " + event.getLargeTable().getTableName() +
                    ", number of partitions (" + numPartition + ")");

            MultiOutputInfo outputInfo = event.getOutput();
            StorageInfo storageInfo = outputInfo.getStorageInfo();
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

            try
            {
                if (minio == null && storageInfo.getScheme() == Storage.Scheme.minio)
                {
                    ConfigMinIO(storageInfo.getEndpoint(), storageInfo.getAccessKey(), storageInfo.getSecretKey());
                    minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                }
            } catch (Exception e)
            {
                throw new PixelsWorkerException("failed to initialize MinIO storage", e);
            }
            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            getFileSchema(threadPool, s3, leftSchema, rightSchema,
                    leftPartitioned.get(0), rightPartitioned.get(0), true);
            Joiner joiner = new Joiner(joinType,
                    leftSchema.get(), leftColAlias, leftProjection, leftKeyColumnIds,
                    rightSchema.get(), rightColAlias, rightProjection, rightKeyColumnIds);
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
                        buildHashTable(queryId, joiner, parts, leftCols, hashValues, numPartition, metricsCollector);
                    }
                    catch (Exception e)
                    {
                        throw new PixelsWorkerException("error during hash table construction", e);
                    }
                }));
            }
            for (Future future : leftFutures)
            {
                future.get();
            }
            logger.info("hash table size: " + joiner.getSmallTableSize() + ", duration (ns): " +
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
                                            queryId, joiner, parts, rightCols, hashValues,
                                            numPartition, outputPartitionInfo, result, metricsCollector) :
                                    joinWithRightTable(queryId, joiner, parts, rightCols,
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

            String outputPath = outputFolder + outputInfo.getFileNames().get(0);
            try
            {
                MetricsCollector.Timer writeCostTimer = new MetricsCollector.Timer().start();
                PixelsWriter pixelsWriter;
                if (partitionOutput)
                {
                    pixelsWriter = getWriter(joiner.getJoinedSchema(),
                            storageInfo.getScheme() == Storage.Scheme.minio ? minio : s3, outputPath,
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
                    pixelsWriter = getWriter(joiner.getJoinedSchema(),
                            storageInfo.getScheme() == Storage.Scheme.minio ? minio : s3, outputPath,
                            encoding, false, null);
                    ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result.get(0);
                    for (VectorizedRowBatch rowBatch : rowBatches)
                    {
                        pixelsWriter.addRowBatch(rowBatch);
                    }
                }
                pixelsWriter.close();
                metricsCollector.addWriteBytes(pixelsWriter.getCompletedBytes());
                metricsCollector.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                if (storageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!minio.exists(outputPath))
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
                        pixelsWriter = getWriter(joiner.getJoinedSchema(),
                                storageInfo.getScheme() == Storage.Scheme.minio ? minio : s3, outputPath,
                                encoding, true, Arrays.stream(
                                        outputPartitionInfo.getKeyColumnIds()).boxed().
                                        collect(Collectors.toList()));
                        joiner.writeLeftOuterAndPartition(pixelsWriter, rowBatchSize, outputPartitionInfo);
                    }
                    else
                    {
                        pixelsWriter = getWriter(joiner.getJoinedSchema(),
                                storageInfo.getScheme() == Storage.Scheme.minio ? minio : s3, outputPath,
                                encoding, false, null);
                        joiner.writeLeftOuter(pixelsWriter, rowBatchSize);
                    }
                    pixelsWriter.close();
                    metricsCollector.addWriteBytes(pixelsWriter.getCompletedBytes());
                    metricsCollector.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                    joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                    if (storageInfo.getScheme() == Storage.Scheme.minio)
                    {
                        while (!minio.exists(outputPath))
                        {
                            // Wait for 10ms and see if the output file is visible.
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                }
                metricsCollector.addOutputCostNs(writeCostTimer.stop());
            } catch (Exception e)
            {
                throw new PixelsWorkerException(
                        "failed to finish writing and close the join result file '" + outputPath + "'", e);
            }

            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            setPerfMetrics(joinOutput, metricsCollector);
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

    /**
     * Scan the partitioned file of the left table and populate the hash table for the join.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for which the hash table is built
     * @param leftParts the information of partitioned files of the left table
     * @param leftCols the column names of the left table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param metricsCollector the collector of the performance metrics
     */
    protected static void buildHashTable(long queryId, Joiner joiner, List<String> leftParts, String[] leftCols,
                                         List<Integer> hashValues, int numPartition, MetricsCollector metricsCollector)
    {
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeCostTimer = new MetricsCollector.Timer();
        while (!leftParts.isEmpty())
        {
            for (Iterator<String> it = leftParts.iterator(); it.hasNext(); )
            {
                String leftPartitioned = it.next();
                try
                {
                    if (exists(s3, leftPartitioned))
                    {
                        it.remove();
                    } else
                    {
                        TimeUnit.MILLISECONDS.sleep(10);
                        continue;
                    }
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to check the existence of the partitioned file '" +
                            leftPartitioned + "' of the left table", e);
                }

                readCostTimer.start();
                try (PixelsReader pixelsReader = getReader(leftPartitioned, s3))
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
                        PixelsReaderOption option = getReaderOption(queryId, leftCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rowBatch;
                        readCostTimer.start();
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        readCostTimer.stop();
                        checkArgument(recordReader.isValid(), "failed to get record reader");
                        metricsCollector.addReadBytes(recordReader.getCompletedBytes());
                        metricsCollector.addNumReadRequests(recordReader.getNumReadRequests());

                        computeCostTimer.start();
                        do
                        {
                            rowBatch = recordReader.readBatch(rowBatchSize);
                            if (rowBatch.size > 0)
                            {
                                joiner.populateLeftTable(rowBatch);
                            }
                        } while (!rowBatch.endOfFile);
                        computeCostTimer.stop();
                    }
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to scan the partitioned file '" +
                            leftPartitioned + "' and build the hash table", e);
                }
            }
        }
        metricsCollector.addComputeCostNs(computeCostTimer.getDuration());
        metricsCollector.addInputCostNs(readCostTimer.getDuration());
    }

    /**
     * Scan the partitioned file of the right table and do the join.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for the partitioned join
     * @param rightParts the information of partitioned files of the right table
     * @param rightCols the column names of the right table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param joinResult the container of the join result
     * @param metricsCollector the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    protected static int joinWithRightTable(
            long queryId, Joiner joiner, List<String> rightParts, String[] rightCols, List<Integer> hashValues,
            int numPartition, ConcurrentLinkedQueue<VectorizedRowBatch> joinResult, MetricsCollector metricsCollector)
    {
        int joinedRows = 0;
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeCostTimer = new MetricsCollector.Timer();
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                try
                {
                    if (exists(s3, rightPartitioned))
                    {
                        it.remove();
                    } else
                    {
                        TimeUnit.MILLISECONDS.sleep(10);
                        continue;
                    }
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to check the existence of the partitioned file '" +
                            rightPartitioned + "' of the right table", e);
                }

                readCostTimer.start();
                try (PixelsReader pixelsReader = getReader(rightPartitioned, s3))
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
                        PixelsReaderOption option = getReaderOption(queryId, rightCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rowBatch;
                        readCostTimer.start();
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        readCostTimer.stop();
                        checkArgument(recordReader.isValid(), "failed to get record reader");
                        metricsCollector.addReadBytes(recordReader.getCompletedBytes());
                        metricsCollector.addNumReadRequests(recordReader.getNumReadRequests());

                        computeCostTimer.start();
                        do
                        {
                            rowBatch = recordReader.readBatch(rowBatchSize);
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
                    }
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
                }
            }
        }

        metricsCollector.addInputCostNs(readCostTimer.getDuration());
        metricsCollector.addComputeCostNs(computeCostTimer.getDuration());
        return joinedRows;
    }

    /**
     * Scan the partitioned file of the right table, do the join, and partition the output.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for the partitioned join
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
            long queryId, Joiner joiner, List<String> rightParts, String[] rightCols, List<Integer> hashValues, int numPartition,
            PartitionInfo postPartitionInfo, List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult, MetricsCollector metricsCollector)
    {
        requireNonNull(postPartitionInfo, "outputPartitionInfo is null");
        Partitioner partitioner = new Partitioner(postPartitionInfo.getNumPartition(),
                rowBatchSize, joiner.getJoinedSchema(), postPartitionInfo.getKeyColumnIds());
        int joinedRows = 0;
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeCostTimer = new MetricsCollector.Timer();
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                try
                {
                    if (exists(s3, rightPartitioned))
                    {
                        it.remove();
                    } else
                    {
                        TimeUnit.MILLISECONDS.sleep(10);
                        continue;
                    }
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to check the existence of the partitioned file '" +
                            rightPartitioned + "' of the right table", e);
                }

                readCostTimer.start();
                try (PixelsReader pixelsReader = getReader(rightPartitioned, s3))
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
                        PixelsReaderOption option = getReaderOption(queryId, rightCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rowBatch;
                        readCostTimer.start();
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        readCostTimer.stop();
                        checkArgument(recordReader.isValid(), "failed to get record reader");
                        metricsCollector.addReadBytes(recordReader.getCompletedBytes());
                        metricsCollector.addNumReadRequests(recordReader.getNumReadRequests());

                        computeCostTimer.start();
                        do
                        {
                            rowBatch = recordReader.readBatch(rowBatchSize);
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
                    }
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
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

        metricsCollector.addInputCostNs(readCostTimer.getDuration());
        metricsCollector.addComputeCostNs(computeCostTimer.getDuration());
        return joinedRows;
    }
}
