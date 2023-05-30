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

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
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
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
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
public class BaseBroadcastJoinWorker extends Worker<BroadcastJoinInput, JoinOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseBroadcastJoinWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    /**
     * Scan the input files of the left table and populate the hash table for the join.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param joiner the joiner for which the hash table is built
     * @param leftInputs the information of input files of the left table,
     *                   the list <b>must be mutable</b>
     * @param leftScheme the storage scheme of the left table
     * @param checkExistence whether check the existence of the input files
     * @param leftCols the column names of the left table
     * @param leftFilter the table scan filter on the left table
     * @param workerMetrics the collector of the performance metrics
     */
    public static void buildHashTable(long transId, Joiner joiner, List<InputInfo> leftInputs,
                                      Storage.Scheme leftScheme, boolean checkExistence, String[] leftCols,
                                      TableScanFilter leftFilter, WorkerMetrics workerMetrics)
    {
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!leftInputs.isEmpty())
        {
            for (Iterator<InputInfo> it = leftInputs.iterator(); it.hasNext(); )
            {
                InputInfo input = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        input.getPath(), WorkerCommon.getStorage(leftScheme)))
                {
                    readCostTimer.stop();
                    if (input.getRgStart() >= pixelsReader.getRowGroupNum())
                    {
                        it.remove();
                        continue;
                    }
                    if (input.getRgStart() + input.getRgLength() >= pixelsReader.getRowGroupNum() ||
                            input.getRgLength() <= 0)
                    {
                        input.setRgLength(pixelsReader.getRowGroupNum() - input.getRgStart());
                    }
                    PixelsReaderOption option = WorkerCommon.getReaderOption(transId, leftCols, input);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");

                    Bitmap filtered = new Bitmap(WorkerCommon.rowBatchSize, true);
                    Bitmap tmp = new Bitmap(WorkerCommon.rowBatchSize, false);
                    computeCostTimer.start();
                    do
                    {
                        rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
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
                    it.remove();
                } catch (Exception e)
                {
                    if (checkExistence && e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the left table input file '" +
                            input.getPath() + "' and build the hash table", e);
                }
            }
            if (checkExistence && !leftInputs.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the input files");
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
    }

    /**
     * Scan the input files of the right table and do the join.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param joiner the joiner for the broadcast join
     * @param rightInputs the information of input files of the right table,
     *                    the list <b>must be mutable</b>
     * @param rightScheme the storage scheme of the right table
     * @param checkExistence whether check the existence of the input files
     * @param rightCols the column names of the right table
     * @param rightFilter the table scan filter on the right table
     * @param joinResult the container of the join result
     * @param workerMetrics the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    public static int joinWithRightTable(
            long transId, Joiner joiner, List<InputInfo> rightInputs, Storage.Scheme rightScheme,
            boolean checkExistence, String[] rightCols, TableScanFilter rightFilter,
            ConcurrentLinkedQueue<VectorizedRowBatch> joinResult, WorkerMetrics workerMetrics)
    {
        int joinedRows = 0;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightInputs.isEmpty())
        {
            for (Iterator<InputInfo> it = rightInputs.iterator(); it.hasNext(); )
            {
                InputInfo input = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        input.getPath(), WorkerCommon.getStorage(rightScheme)))
                {
                    readCostTimer.stop();
                    if (input.getRgStart() >= pixelsReader.getRowGroupNum())
                    {
                        it.remove();
                        continue;
                    }
                    if (input.getRgStart() + input.getRgLength() >= pixelsReader.getRowGroupNum())
                    {
                        input.setRgLength(pixelsReader.getRowGroupNum() - input.getRgStart());
                    }
                    PixelsReaderOption option = WorkerCommon.getReaderOption(transId, rightCols, input);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");

                    Bitmap filtered = new Bitmap(WorkerCommon.rowBatchSize, true);
                    Bitmap tmp = new Bitmap(WorkerCommon.rowBatchSize, false);
                    computeCostTimer.start();
                    do
                    {
                        rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
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
                    it.remove();
                } catch (Exception e)
                {
                    if (checkExistence && e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the right table input file '" +
                            input.getPath() + "' and do the join", e);
                }
            }
            if (checkExistence && !rightInputs.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the input files");
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
     * Scan the input files of the right table, do the join, and partition the result.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param joiner the joiner for the broadcast join
     * @param rightInputs the information of input files of the right table,
     *                    the list <b>must be mutable</b>
     * @param rightScheme the storage scheme of the right table
     * @param checkExistence whether check the existence of the input files
     * @param rightCols the column names of the right table
     * @param rightFilter the table scan filter on the right table
     * @param postPartitionInfo the partition information of post partitioning
     * @param partitionResult the container of the join and post partitioning result
     * @param workerMetrics the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    public static int joinWithRightTableAndPartition(
            long transId, Joiner joiner, List<InputInfo> rightInputs, Storage.Scheme rightScheme,
            boolean checkExistence, String[] rightCols, TableScanFilter rightFilter, PartitionInfo postPartitionInfo,
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
        while (!rightInputs.isEmpty())
        {
            for (Iterator<InputInfo> it = rightInputs.iterator(); it.hasNext(); )
            {
                InputInfo input = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        input.getPath(), WorkerCommon.getStorage(rightScheme)))
                {
                    readCostTimer.stop();
                    if (input.getRgStart() >= pixelsReader.getRowGroupNum())
                    {
                        it.remove();
                        continue;
                    }
                    if (input.getRgStart() + input.getRgLength() >= pixelsReader.getRowGroupNum())
                    {
                        input.setRgLength(pixelsReader.getRowGroupNum() - input.getRgStart());
                    }
                    PixelsReaderOption option = WorkerCommon.getReaderOption(transId, rightCols, input);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");

                    Bitmap filtered = new Bitmap(WorkerCommon.rowBatchSize, true);
                    Bitmap tmp = new Bitmap(WorkerCommon.rowBatchSize, false);
                    computeCostTimer.start();
                    do
                    {
                        rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
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
                    it.remove();
                } catch (Exception e)
                {
                    if (checkExistence && e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the right table input file '" +
                            input.getPath() + "' and do the join", e);
                }
            }
            if (checkExistence && !rightInputs.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the input files");
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

    @Override
    public JoinOutput process(BroadcastJoinInput event)
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
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);

            long transId = event.getTransId();
            BroadcastTableInfo leftTable = requireNonNull(event.getSmallTable(), "leftTable is null");
            StorageInfo leftInputStorageInfo = requireNonNull(leftTable.getStorageInfo(), "leftStorageInfo is null");
            List<InputSplit> leftInputs = requireNonNull(leftTable.getInputSplits(), "leftInputs is null");
            checkArgument(leftInputs.size() > 0, "left table is empty");
            String[] leftCols = leftTable.getColumnsToRead();
            int[] leftKeyColumnIds = leftTable.getKeyColumnIds();
            TableScanFilter leftFilter = JSON.parseObject(leftTable.getFilter(), TableScanFilter.class);

            BroadcastTableInfo rightTable = requireNonNull(event.getLargeTable(), "rightTable is null");
            StorageInfo rightInputStorageInfo = requireNonNull(rightTable.getStorageInfo(), "rightStorageInfo is null");
            List<InputSplit> rightInputs = requireNonNull(rightTable.getInputSplits(), "rightInputs is null");
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
            StorageInfo outputStorageInfo = outputInfo.getStorageInfo();
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

            WorkerCommon.initStorage(leftInputStorageInfo);
            WorkerCommon.initStorage(rightInputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            boolean partitionOutput = event.getJoinInfo().isPostPartition();
            PartitionInfo outputPartitionInfo = event.getJoinInfo().getPostPartitionInfo();
            if (partitionOutput)
            {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }

            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            WorkerCommon.getFileSchemaFromSplits(threadPool,
                    WorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                    WorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                    leftSchema, rightSchema, leftInputs, rightInputs);
            Joiner joiner = new Joiner(joinType,
                    WorkerCommon.getResultSchema(leftSchema.get(), leftCols), leftColAlias, leftProjection, leftKeyColumnIds,
                    WorkerCommon.getResultSchema(rightSchema.get(), rightCols), rightColAlias, rightProjection, rightKeyColumnIds);
            // build the hash table for the left table.
            List<Future> leftFutures = new ArrayList<>();
            for (InputSplit inputSplit : leftInputs)
            {
                List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                leftFutures.add(threadPool.submit(() -> {
                    try
                    {
                        buildHashTable(transId, joiner, inputs, leftInputStorageInfo.getScheme(),
                                !leftTable.isBase(), leftCols, leftFilter, workerMetrics);
                    }
                    catch (Exception e)
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
                for (InputSplit inputSplit : rightInputs)
                {
                    List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                    threadPool.execute(() -> {
                        try
                        {
                            int numJoinedRows = partitionOutput ?
                                    joinWithRightTableAndPartition(
                                            transId, joiner, inputs, rightInputStorageInfo.getScheme(),
                                            !rightTable.isBase(), rightCols, rightFilter,
                                            outputPartitionInfo, result, workerMetrics) :
                                    joinWithRightTable(transId, joiner, inputs, rightInputStorageInfo.getScheme(),
                                            !rightTable.isBase(), rightCols, rightFilter, result.get(0), workerMetrics);
                        } catch (Exception e)
                        {
                            throw new WorkerException("error during broadcast join", e);
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
            } catch (Exception e)
            {
                throw new WorkerException(
                        "failed to finish writing and close the join result file '" + outputPath + "'", e);
            }

            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(joinOutput, workerMetrics);
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

}
