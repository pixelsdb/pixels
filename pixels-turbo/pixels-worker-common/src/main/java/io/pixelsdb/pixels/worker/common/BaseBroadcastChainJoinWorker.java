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
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastChainJoinInput;
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
 * Broadcast chain join is the combination of a set of broadcast joins.
 * All the left tables in a chain join are broadcast.
 *
 * @author hank
 * @create 2022-06-03
 * @update 2023-04-23 (moved from pixels-worker-lambda to here as the base worker implementation)
 */
public class BaseBroadcastChainJoinWorker extends Worker<BroadcastChainJoinInput, JoinOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseBroadcastChainJoinWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    /**
     * Build the joiner for the last join, i.e., the join between the join result of
     * the left tables and the right table.
     * @param transId the transaction id
     * @param executor the thread pool
     * @param leftTables the information of the left tables
     * @param chainJoinInfos the information of the chain joins between the left tables
     * @param rightTable the information of the right table, a.k.a., the last table to join with
     * @param lastJoinInfo the information of the last join
     * @param workerMetrics the collector of the performance metrics
     * @return the joiner of the last join
     */
    private static Joiner buildJoiner(
            long transId, ExecutorService executor, List<BroadcastTableInfo> leftTables, List<ChainJoinInfo> chainJoinInfos,
            BroadcastTableInfo rightTable, JoinInfo lastJoinInfo, WorkerMetrics workerMetrics)
    {
        try
        {
            WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
            BroadcastTableInfo t1 = leftTables.get(0);
            BroadcastTableInfo t2 = leftTables.get(1);
            Joiner currJoiner = buildFirstJoiner(transId, executor, t1, t2, chainJoinInfos.get(0), workerMetrics);
            for (int i = 1; i < leftTables.size() - 1; ++i)
            {
                BroadcastTableInfo currRightTable = leftTables.get(i);
                BroadcastTableInfo nextTable = leftTables.get(i+1);
                readCostTimer.start();
                TypeDescription nextTableSchema = WorkerCommon.getFileSchemaFromSplits(
                        WorkerCommon.getStorage(nextTable.getStorageInfo().getScheme()), nextTable.getInputSplits());
                ChainJoinInfo currJoinInfo = chainJoinInfos.get(i-1);
                ChainJoinInfo nextJoinInfo = chainJoinInfos.get(i);
                TypeDescription nextResultSchema = WorkerCommon.getResultSchema(nextTableSchema, nextTable.getColumnsToRead());
                readCostTimer.stop();
                Joiner nextJoiner = new Joiner(nextJoinInfo.getJoinType(),
                        currJoiner.getJoinedSchema(), nextJoinInfo.getSmallColumnAlias(),
                        nextJoinInfo.getSmallProjection(), currJoinInfo.getKeyColumnIds(),
                        nextResultSchema, nextJoinInfo.getLargeColumnAlias(),
                        nextJoinInfo.getLargeProjection(), nextTable.getKeyColumnIds());
                chainJoin(transId, executor, currJoiner, nextJoiner, currRightTable, workerMetrics);
                currJoiner = nextJoiner;
            }
            ChainJoinInfo lastChainJoin = chainJoinInfos.get(chainJoinInfos.size()-1);
            BroadcastTableInfo lastLeftTable = leftTables.get(leftTables.size()-1);
            TypeDescription rightTableSchema = WorkerCommon.getFileSchemaFromSplits(
                    WorkerCommon.getStorage(rightTable.getStorageInfo().getScheme()), rightTable.getInputSplits());
            TypeDescription rightResultSchema = WorkerCommon.getResultSchema(rightTableSchema, rightTable.getColumnsToRead());
            Joiner finalJoiner = new Joiner(lastJoinInfo.getJoinType(),
                    currJoiner.getJoinedSchema(), lastJoinInfo.getSmallColumnAlias(),
                    lastJoinInfo.getSmallProjection(), lastChainJoin.getKeyColumnIds(),
                    rightResultSchema, lastJoinInfo.getLargeColumnAlias(),
                    lastJoinInfo.getLargeProjection(), rightTable.getKeyColumnIds());
            chainJoin(transId, executor, currJoiner, finalJoiner, lastLeftTable, workerMetrics);
            workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
            return finalJoiner;
        } catch (Exception e)
        {
            throw new WorkerException("failed to join left tables", e);
        }
    }

    /**
     * Build the joiner for the join between the first two left tables.
     *
     * @param transId the transaction id
     * @param executor the thread pool
     * @param t1 the information of the first left table
     * @param t2 the information of the second left table
     * @param joinInfo the information of the join between t1 and t2
     * @param workerMetrics the collector of the performance metrics
     * @return the joiner of the first join
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected static Joiner buildFirstJoiner(
            long transId, ExecutorService executor, BroadcastTableInfo t1, BroadcastTableInfo t2,
            ChainJoinInfo joinInfo, WorkerMetrics workerMetrics) throws ExecutionException, InterruptedException
    {
        AtomicReference<TypeDescription> t1Schema = new AtomicReference<>();
        AtomicReference<TypeDescription> t2Schema = new AtomicReference<>();
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer().start();
        WorkerCommon.getFileSchemaFromSplits(executor,
                WorkerCommon.getStorage(t1.getStorageInfo().getScheme()),
                WorkerCommon.getStorage(t2.getStorageInfo().getScheme()),
                t1Schema, t2Schema, t1.getInputSplits(), t2.getInputSplits());
        Joiner joiner = new Joiner(joinInfo.getJoinType(),
                WorkerCommon.getResultSchema(t1Schema.get(), t1.getColumnsToRead()), joinInfo.getSmallColumnAlias(),
                joinInfo.getSmallProjection(), t1.getKeyColumnIds(),
                WorkerCommon.getResultSchema(t2Schema.get(), t2.getColumnsToRead()), joinInfo.getLargeColumnAlias(),
                joinInfo.getLargeProjection(), t2.getKeyColumnIds());
        workerMetrics.addInputCostNs(readCostTimer.stop());
        List<Future> leftFutures = new ArrayList<>();
        TableScanFilter t1Filter = JSON.parseObject(t1.getFilter(), TableScanFilter.class);
        for (InputSplit inputSplit : t1.getInputSplits())
        {
            List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
            leftFutures.add(executor.submit(() -> {
                try
                {
                    BaseBroadcastJoinWorker.buildHashTable(transId, joiner, inputs, t1.getStorageInfo().getScheme(),
                            !t1.isBase(), t1.getColumnsToRead(), t1Filter, workerMetrics);
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
        return joiner;
    }

    /**
     * Perform the chain join between two left tables and use the join result to
     * populate the hash table of the next join.
     *
     * @param transId the transaction id
     * @param executor the thread pool
     * @param currJoiner the joiner of the two left tables
     * @param nextJoiner the joiner of the next join
     * @param currRightTable the right table in the two left tables
     * @param workerMetrics the collector of the performance metrics
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected static void chainJoin(long transId, ExecutorService executor, Joiner currJoiner, Joiner nextJoiner,
                                    BroadcastTableInfo currRightTable, WorkerMetrics workerMetrics)
            throws ExecutionException, InterruptedException
    {
        TableScanFilter currRightFilter = JSON.parseObject(currRightTable.getFilter(), TableScanFilter.class);
        List<Future> rightFutures = new ArrayList<>();
        for (InputSplit inputSplit : currRightTable.getInputSplits())
        {
            List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
            rightFutures.add(executor.submit(() -> {
                try
                {
                    chainJoinSplit(transId, currJoiner, nextJoiner, inputs,
                            currRightTable.getStorageInfo().getScheme(), !currRightTable.isBase(),
                            currRightTable.getColumnsToRead(), currRightFilter, workerMetrics);
                }
                catch (Exception e)
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

    /**
     * Perform the join of two left tables on one split of the right one.
     *
     * @param transId the transaction id
     * @param currJoiner the joiner of the two left tables
     * @param nextJoiner the joiner of the next join
     * @param rightInputs the information of the input files in the split of the right one
     *                   of the two left tables
     * @param rightScheme the storage scheme of the right table
     * @param checkExistence whether check the existence of the input files
     * @param rightCols the column names of the right one of the two left tables
     * @param rightFilter the filter of the right one of the two left tables
     * @param workerMetrics the collector of the performance metrics
     */
    private static void chainJoinSplit(
            long transId, Joiner currJoiner, Joiner nextJoiner, List<InputInfo> rightInputs,
            Storage.Scheme rightScheme, boolean checkExistence, String[] rightCols,
            TableScanFilter rightFilter, WorkerMetrics workerMetrics)
    {
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
                            List<VectorizedRowBatch> joinedBatches = currJoiner.join(rowBatch);
                            for (VectorizedRowBatch joined : joinedBatches)
                            {
                                if (!joined.isEmpty())
                                {
                                    nextJoiner.populateLeftTable(joined);
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
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
    }

    @Override
    public JoinOutput process(BroadcastChainJoinInput event)
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
            List<BroadcastTableInfo> chainTables = event.getChainTables();
            List<ChainJoinInfo> chainJoinInfos = event.getChainJoinInfos();
            requireNonNull(chainTables, "chainTables is null");
            requireNonNull(chainJoinInfos, "chainJoinInfos is null");
            checkArgument(chainTables.size() == chainJoinInfos.size() + 1,
                    "left table num is not consistent with (chain-join info num + 1).");
            checkArgument(chainTables.size() > 1, "there should be at least two chain tables");

            BroadcastTableInfo rightTable = requireNonNull(event.getLargeTable(), "rightTable is null");
            StorageInfo rightInputStorageInfo = requireNonNull(rightTable.getStorageInfo(),
                    "rightInputStorageInto is null");
            List<InputSplit> rightInputs = requireNonNull(rightTable.getInputSplits(), "rightInputs is null");
            checkArgument(rightInputs.size() > 0, "rightPartitioned is empty");
            String[] rightCols = rightTable.getColumnsToRead();
            TableScanFilter rightFilter = JSON.parseObject(rightTable.getFilter(), TableScanFilter.class);

            JoinInfo lastJoinInfo = event.getJoinInfo();
            JoinType joinType = lastJoinInfo.getJoinType();
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

            logger.info("large table: " + event.getLargeTable().getTableName());

            for (TableInfo tableInfo : chainTables)
            {
                WorkerCommon.initStorage(tableInfo.getStorageInfo());
            }
            WorkerCommon.initStorage(rightInputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            boolean partitionOutput = event.getJoinInfo().isPostPartition();
            PartitionInfo outputPartitionInfo = event.getJoinInfo().getPostPartitionInfo();
            if (partitionOutput)
            {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }

            // build the joiner.
            Joiner joiner = buildJoiner(transId, threadPool, chainTables, chainJoinInfos,
                    rightTable, lastJoinInfo, workerMetrics);
            logger.info("chain hash table size: " + joiner.getSmallTableSize() + ", duration (ns): " +
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
                                    BaseBroadcastJoinWorker.joinWithRightTableAndPartition(
                                            transId, joiner, inputs, rightInputStorageInfo.getScheme(),
                                            !rightTable.isBase(), rightCols, rightFilter,
                                            outputPartitionInfo, result, workerMetrics) :
                                    BaseBroadcastJoinWorker.joinWithRightTable(transId, joiner, inputs,
                                            rightInputStorageInfo.getScheme(), !rightTable.isBase(), rightCols,
                                            rightFilter, result.get(0), workerMetrics);
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
