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

import com.alibaba.fastjson.JSON;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.executor.join.Partitioner;
import io.pixelsdb.pixels.executor.lambda.domain.*;
import io.pixelsdb.pixels.executor.lambda.input.BroadcastJoinInput;
import io.pixelsdb.pixels.executor.lambda.output.JoinOutput;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.Minio.ConfigMinio;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 07/05/2022
 */
public class BroadcastJoinWorker implements RequestHandler<BroadcastJoinInput, JoinOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(BroadcastJoinWorker.class);
    private final MetricsCollector metricsCollector = new MetricsCollector();

    @Override
    public JoinOutput handleRequest(BroadcastJoinInput event, Context context)
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

            BroadcastTableInfo leftTable = event.getSmallTable();
            List<InputSplit> leftInputs = leftTable.getInputSplits();
            requireNonNull(leftInputs, "leftInputs is null");
            checkArgument(leftInputs.size() > 0, "left table is empty");
            String[] leftCols = leftTable.getColumnsToRead();
            int[] leftKeyColumnIds = leftTable.getKeyColumnIds();
            TableScanFilter leftFilter = JSON.parseObject(leftTable.getFilter(), TableScanFilter.class);

            BroadcastTableInfo rightTable = event.getLargeTable();
            List<InputSplit> rightInputs = rightTable.getInputSplits();
            requireNonNull(rightInputs, "rightInputs is null");
            checkArgument(rightInputs.size() > 0, "right table is empty");
            String[] rightCols = rightTable.getColumnsToRead();
            int[] rightKeyColumnIds = rightTable.getKeyColumnIds();
            TableScanFilter rightFilter = JSON.parseObject(rightTable.getFilter(), TableScanFilter.class);

            String[] leftColAlias = event.getJoinInfo().getSmallColumnAlias();
            String[] rightColAlias = event.getJoinInfo().getLargeColumnAlias();
            boolean[] leftProjection = event.getJoinInfo().getSmallProjection();
            boolean[] rightProjection = event.getJoinInfo().getLargeProjection();
            JoinType joinType = event.getJoinInfo().getJoinType();
            checkArgument(joinType != JoinType.EQUI_LEFT && joinType != JoinType.EQUI_FULL,
                    "broadcast join can not be used for LEFT_OUTER or FULL_OUTER join");

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

            logger.info("small table: " + event.getSmallTable().getTableName() +
                    "', large table: " + event.getLargeTable().getTableName());

            try
            {
                if (minio == null && storageInfo.getScheme() == Storage.Scheme.minio)
                {
                    ConfigMinio(storageInfo.getEndpoint(), storageInfo.getAccessKey(), storageInfo.getSecretKey());
                    minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                }
            } catch (Exception e)
            {
                throw new PixelsWorkerException("failed to initialize Minio storage", e);
            }

            boolean partitionOutput = event.getJoinInfo().isPostPartition();
            PartitionInfo outputPartitionInfo = event.getJoinInfo().getPostPartitionInfo();
            if (partitionOutput)
            {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }

            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            getFileSchema(threadPool, s3, leftSchema, rightSchema,
                    leftInputs.get(0).getInputInfos().get(0).getPath(),
                    rightInputs.get(0).getInputInfos().get(0).getPath(), !(leftTable.isBase() && rightTable.isBase()));
            Joiner joiner = new Joiner(joinType,
                    getResultSchema(leftSchema.get(), leftCols), leftColAlias, leftProjection, leftKeyColumnIds,
                    getResultSchema(rightSchema.get(), rightCols), rightColAlias, rightProjection, rightKeyColumnIds);
            // build the hash table for the left table.
            List<Future> leftFutures = new ArrayList<>();
            for (InputSplit inputSplit : leftInputs)
            {
                List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                leftFutures.add(threadPool.submit(() -> {
                    try
                    {
                        buildHashTable(queryId, joiner, inputs, !leftTable.isBase(), leftCols, leftFilter, metricsCollector);
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
                for (InputSplit inputSplit : rightInputs)
                {
                    List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                    threadPool.execute(() -> {
                        try
                        {
                            int numJoinedRows = partitionOutput ?
                                    joinWithRightTableAndPartition(
                                            queryId, joiner, inputs, !rightTable.isBase(), rightCols, rightFilter,
                                            outputPartitionInfo, result, metricsCollector) :
                                    joinWithRightTable(queryId, joiner, inputs, !rightTable.isBase(), rightCols,
                                            rightFilter, result.get(0), metricsCollector);
                        } catch (Exception e)
                        {
                            throw new PixelsWorkerException("error during broadcast join", e);
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
                PixelsWriter pixelsWriter;
                MetricsCollector.Timer writeCostTimer = new MetricsCollector.Timer().start();
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
                joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                if (storageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!minio.exists(outputPath))
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
     * Scan the input files of the left table and populate the hash table for the join.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for which the hash table is built
     * @param leftInputs the information of input files of the left table,
     *                   the list <b>must be mutable</b>
     * @param checkExistence whether check the existence of the input files
     * @param leftCols the column names of the left table
     * @param leftFilter the table scan filter on the left table
     * @param metricsCollector the collector of the performance metrics
     */
    public static void buildHashTable(long queryId, Joiner joiner, List<InputInfo> leftInputs, boolean checkExistence,
                                      String[] leftCols, TableScanFilter leftFilter, MetricsCollector metricsCollector)
    {
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeCostTimer = new MetricsCollector.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!leftInputs.isEmpty())
        {
            for (Iterator<InputInfo> it = leftInputs.iterator(); it.hasNext(); )
            {
                InputInfo input = it.next();
                if (checkExistence)
                {
                    try
                    {
                        if (exists(s3, input.getPath()))
                        {
                            it.remove();
                        } else
                        {
                            TimeUnit.MILLISECONDS.sleep(10);
                            continue;
                        }
                    } catch (Exception e)
                    {
                        throw new PixelsWorkerException(
                                "failed to check the existence of the left table input file '" +
                                input.getPath() + "'", e);
                    }
                }
                else
                {
                    it.remove();
                }

                readCostTimer.start();
                try (PixelsReader pixelsReader = getReader(input.getPath(), s3))
                {
                    readCostTimer.stop();
                    if (input.getRgStart() >= pixelsReader.getRowGroupNum())
                    {
                        continue;
                    }
                    if (input.getRgStart() + input.getRgLength() >= pixelsReader.getRowGroupNum() ||
                            input.getRgLength() <= 0)
                    {
                        input.setRgLength(pixelsReader.getRowGroupNum() - input.getRgStart());
                    }
                    PixelsReaderOption option = getReaderOption(queryId, leftCols, input);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");

                    Bitmap filtered = new Bitmap(rowBatchSize, true);
                    Bitmap tmp = new Bitmap(rowBatchSize, false);
                    computeCostTimer.start();
                    do
                    {
                        rowBatch = recordReader.readBatch(rowBatchSize);
                        leftFilter.doFilter(rowBatch, filtered, tmp);
                        rowBatch.applyFilter(filtered);
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
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to scan the left table input file '" +
                            input.getPath() + "' and build the hash table", e);
                }
            }
        }
        metricsCollector.addReadBytes(readBytes);
        metricsCollector.addNumReadRequests(numReadRequests);
        metricsCollector.addComputeCostNs(computeCostTimer.getElapsedNs());
        metricsCollector.addInputCostNs(readCostTimer.getElapsedNs());
    }

    /**
     * Scan the input files of the right table and do the join.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for the broadcast join
     * @param rightInputs the information of input files of the right table,
     *                    the list <b>must be mutable</b>
     * @param checkExistence whether check the existence of the input files
     * @param rightCols the column names of the right table
     * @param rightFilter the table scan filter on the right table
     * @param joinResult the container of the join result
     * @param metricsCollector the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    public static int joinWithRightTable(
            long queryId, Joiner joiner, List<InputInfo> rightInputs, boolean checkExistence, String[] rightCols,
            TableScanFilter rightFilter, ConcurrentLinkedQueue<VectorizedRowBatch> joinResult, MetricsCollector metricsCollector)
    {
        int joinedRows = 0;
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeCostTimer = new MetricsCollector.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightInputs.isEmpty())
        {
            for (Iterator<InputInfo> it = rightInputs.iterator(); it.hasNext(); )
            {
                InputInfo input = it.next();
                if (checkExistence)
                {
                    try
                    {
                        if (exists(s3, input.getPath()))
                        {
                            it.remove();
                        } else
                        {
                            TimeUnit.MILLISECONDS.sleep(10);
                            continue;
                        }
                    } catch (Exception e)
                    {
                        throw new PixelsWorkerException(
                                "failed to check the existence of the right table input file '" +
                                        input.getPath() + "'", e);
                    }
                }
                else
                {
                    it.remove();
                }

                readCostTimer.start();
                try (PixelsReader pixelsReader = getReader(input.getPath(), s3))
                {
                    readCostTimer.stop();
                    if (input.getRgStart() >= pixelsReader.getRowGroupNum())
                    {
                        continue;
                    }
                    if (input.getRgStart() + input.getRgLength() >= pixelsReader.getRowGroupNum())
                    {
                        input.setRgLength(pixelsReader.getRowGroupNum() - input.getRgStart());
                    }
                    PixelsReaderOption option = getReaderOption(queryId, rightCols, input);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");

                    Bitmap filtered = new Bitmap(rowBatchSize, true);
                    Bitmap tmp = new Bitmap(rowBatchSize, false);
                    computeCostTimer.start();
                    do
                    {
                        rowBatch = recordReader.readBatch(rowBatchSize);
                        rightFilter.doFilter(rowBatch, filtered, tmp);
                        rowBatch.applyFilter(filtered);
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
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to scan the right table input file '" +
                            input.getPath() + "' and do the join", e);
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
     * Scan the input files of the right table, do the join, and partition the result.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for the broadcast join
     * @param rightInputs the information of input files of the right table,
     *                    the list <b>must be mutable</b>
     * @param checkExistence whether check the existence of the input files
     * @param rightCols the column names of the right table
     * @param rightFilter the table scan filter on the right table
     * @param postPartitionInfo the partition information of post partitioning
     * @param partitionResult the container of the join and post partitioning result
     * @param metricsCollector the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    public static int joinWithRightTableAndPartition(
            long queryId, Joiner joiner, List<InputInfo> rightInputs, boolean checkExistence,
            String[] rightCols, TableScanFilter rightFilter, PartitionInfo postPartitionInfo,
            List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult, MetricsCollector metricsCollector)
    {
        requireNonNull(postPartitionInfo, "outputPartitionInfo is null");
        Partitioner partitioner = new Partitioner(postPartitionInfo.getNumPartition(),
                rowBatchSize, joiner.getJoinedSchema(), postPartitionInfo.getKeyColumnIds());
        int joinedRows = 0;
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeCostTimer = new MetricsCollector.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightInputs.isEmpty())
        {
            for (Iterator<InputInfo> it = rightInputs.iterator(); it.hasNext(); )
            {
                InputInfo input = it.next();
                if (checkExistence)
                {
                    try
                    {
                        if (exists(s3, input.getPath()))
                        {
                            it.remove();
                        } else
                        {
                            TimeUnit.MILLISECONDS.sleep(10);
                            continue;
                        }
                    } catch (Exception e)
                    {
                        throw new PixelsWorkerException(
                                "failed to check the existence of the right table input file '" +
                                input.getPath() + "'", e);
                    }
                }
                else
                {
                    it.remove();
                }

                readCostTimer.start();
                try (PixelsReader pixelsReader = getReader(input.getPath(), s3))
                {
                    readCostTimer.stop();
                    if (input.getRgStart() >= pixelsReader.getRowGroupNum())
                    {
                        continue;
                    }
                    if (input.getRgStart() + input.getRgLength() >= pixelsReader.getRowGroupNum())
                    {
                        input.setRgLength(pixelsReader.getRowGroupNum() - input.getRgStart());
                    }
                    PixelsReaderOption option = getReaderOption(queryId, rightCols, input);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");

                    Bitmap filtered = new Bitmap(rowBatchSize, true);
                    Bitmap tmp = new Bitmap(rowBatchSize, false);
                    computeCostTimer.start();
                    do
                    {
                        rowBatch = recordReader.readBatch(rowBatchSize);
                        rightFilter.doFilter(rowBatch, filtered, tmp);
                        rowBatch.applyFilter(filtered);
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
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to scan the right table input file '" +
                            input.getPath() + "' and do the join", e);
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
