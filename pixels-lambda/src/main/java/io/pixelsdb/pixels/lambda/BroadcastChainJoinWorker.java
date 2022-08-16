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
import io.pixelsdb.pixels.executor.lambda.domain.*;
import io.pixelsdb.pixels.executor.lambda.input.BroadcastChainJoinInput;
import io.pixelsdb.pixels.executor.lambda.output.JoinOutput;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.Minio.ConfigMinio;
import static io.pixelsdb.pixels.lambda.BroadcastJoinWorker.*;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;
import static java.util.Objects.requireNonNull;

/**
 * Broadcast chain join is the combination of a set of broadcast joins.
 * All the left tables in a chain join are broadcast.
 *
 * @author hank
 * @date 03/06/2022
 */
public class BroadcastChainJoinWorker implements RequestHandler<BroadcastChainJoinInput, JoinOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(BroadcastChainJoinWorker.class);
    private final MetricsCollector metricsCollector = new MetricsCollector();

    @Override
    public JoinOutput handleRequest(BroadcastChainJoinInput event, Context context)
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
            requireNonNull(chainTables, "chainTables is null");
            requireNonNull(chainJoinInfos, "chainJoinInfos is null");
            checkArgument(chainTables.size() == chainJoinInfos.size()+1,
                    "left table num is not consistent with (chain-join info num + 1).");
            checkArgument(chainTables.size() > 1, "there should be at least two chain tables");

            BroadcastTableInfo rightTable = event.getLargeTable();
            List<InputSplit> rightInputs = rightTable.getInputSplits();
            checkArgument(rightInputs.size() > 0, "rightPartitioned is empty");
            String[] rightCols = rightTable.getColumnsToRead();
            TableScanFilter rightFilter = JSON.parseObject(rightTable.getFilter(), TableScanFilter.class);

            JoinInfo lastJoinInfo = event.getJoinInfo();
            JoinType joinType = lastJoinInfo.getJoinType();
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

            logger.info("large table: " + event.getLargeTable().getTableName());

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
            Joiner joiner = buildJoiner(queryId, threadPool, chainTables, chainJoinInfos,
                    rightTable, lastJoinInfo, metricsCollector);
            logger.info("chain hash table size: " + joiner.getSmallTableSize() + ", duration (ns): " +
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
     * Build the joiner for the last join, i.e., the join between the join result of
     * the left tables and the right table.
     *
     * @param executor the thread pool
     * @param leftTables the information of the left tables
     * @param chainJoinInfos the information of the chain joins between the left tables
     * @param rightTable the information of the right table, a.k.a., the last table to join with
     * @param lastJoinInfo the information of the last join
     * @param metricsCollector the collector of the performance metrics
     * @return the joiner of the last join
     */
    private static Joiner buildJoiner(
            long queryId, ExecutorService executor, List<BroadcastTableInfo> leftTables, List<ChainJoinInfo> chainJoinInfos,
            BroadcastTableInfo rightTable, JoinInfo lastJoinInfo, MetricsCollector metricsCollector)
    {
        try
        {
            MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
            BroadcastTableInfo t1 = leftTables.get(0);
            BroadcastTableInfo t2 = leftTables.get(1);
            Joiner currJoiner = buildFirstJoiner(queryId, executor, t1, t2, chainJoinInfos.get(0), metricsCollector);
            for (int i = 1; i < leftTables.size() - 1; ++i)
            {
                BroadcastTableInfo currRightTable = leftTables.get(i);
                BroadcastTableInfo nextTable = leftTables.get(i+1);
                readCostTimer.start();
                TypeDescription nextTableSchema = WorkerCommon.getFileSchemaFromSplits(s3, nextTable.getInputSplits());
                ChainJoinInfo currJoinInfo = chainJoinInfos.get(i-1);
                ChainJoinInfo nextJoinInfo = chainJoinInfos.get(i);
                TypeDescription nextResultSchema = getResultSchema(nextTableSchema, nextTable.getColumnsToRead());
                readCostTimer.stop();
                Joiner nextJoiner = new Joiner(nextJoinInfo.getJoinType(),
                        currJoiner.getJoinedSchema(), nextJoinInfo.getSmallColumnAlias(),
                        nextJoinInfo.getSmallProjection(), currJoinInfo.getKeyColumnIds(),
                        nextResultSchema, nextJoinInfo.getLargeColumnAlias(),
                        nextJoinInfo.getLargeProjection(), nextTable.getKeyColumnIds());
                chainJoin(queryId, executor, currJoiner, nextJoiner, currRightTable, metricsCollector);
                currJoiner = nextJoiner;
            }
            ChainJoinInfo lastChainJoin = chainJoinInfos.get(chainJoinInfos.size()-1);
            BroadcastTableInfo lastLeftTable = leftTables.get(leftTables.size()-1);
            TypeDescription rightTableSchema = WorkerCommon.getFileSchemaFromSplits(s3, rightTable.getInputSplits());
            TypeDescription rightResultSchema = getResultSchema(rightTableSchema, rightTable.getColumnsToRead());
            Joiner finalJoiner = new Joiner(lastJoinInfo.getJoinType(),
                    currJoiner.getJoinedSchema(), lastJoinInfo.getSmallColumnAlias(),
                    lastJoinInfo.getSmallProjection(), lastChainJoin.getKeyColumnIds(),
                    rightResultSchema, lastJoinInfo.getLargeColumnAlias(),
                    lastJoinInfo.getLargeProjection(), rightTable.getKeyColumnIds());
            chainJoin(queryId, executor, currJoiner, finalJoiner, lastLeftTable, metricsCollector);
            metricsCollector.addInputCostNs(readCostTimer.getElapsedNs());
            return finalJoiner;
        } catch (Exception e)
        {
            throw new PixelsWorkerException("failed to join left tables", e);
        }
    }

    /**
     * Build the joiner for the join between the first two left tables.
     *
     * @param executor the thread pool
     * @param t1 the information of the first left table
     * @param t2 the information of the second left table
     * @param joinInfo the information of the join between t1 and t2
     * @param metricsCollector the collector of the performance metrics
     * @return the joiner of the first join
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected static Joiner buildFirstJoiner(
            long queryId, ExecutorService executor, BroadcastTableInfo t1, BroadcastTableInfo t2,
            ChainJoinInfo joinInfo, MetricsCollector metricsCollector) throws ExecutionException, InterruptedException
    {
        AtomicReference<TypeDescription> t1Schema = new AtomicReference<>();
        AtomicReference<TypeDescription> t2Schema = new AtomicReference<>();
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer().start();
        getFileSchemaFromSplits(executor, s3, t1Schema, t2Schema, t1.getInputSplits(), t2.getInputSplits());
        Joiner joiner = new Joiner(joinInfo.getJoinType(),
                getResultSchema(t1Schema.get(), t1.getColumnsToRead()), joinInfo.getSmallColumnAlias(),
                joinInfo.getSmallProjection(), t1.getKeyColumnIds(),
                getResultSchema(t2Schema.get(), t2.getColumnsToRead()), joinInfo.getLargeColumnAlias(),
                joinInfo.getLargeProjection(), t2.getKeyColumnIds());
        metricsCollector.addInputCostNs(readCostTimer.stop());
        List<Future> leftFutures = new ArrayList<>();
        TableScanFilter t1Filter = JSON.parseObject(t1.getFilter(), TableScanFilter.class);
        for (InputSplit inputSplit : t1.getInputSplits())
        {
            List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
            leftFutures.add(executor.submit(() -> {
                try
                {
                    buildHashTable(queryId, joiner, inputs, !t1.isBase(), t1.getColumnsToRead(), t1Filter, metricsCollector);
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
        logger.info("first left table: " + t1.getTableName());
        return joiner;
    }

    /**
     * Perform the chain join between two left tables and use the join result to
     * populate the hash table of the next join.
     *
     * @param executor the thread pool
     * @param currJoiner the joiner of the two left tables
     * @param nextJoiner the joiner of the next join
     * @param currRightTable the right table in the two left tables
     * @param metricsCollector the collector of the performance metrics
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected static void chainJoin(long queryId, ExecutorService executor, Joiner currJoiner, Joiner nextJoiner,
                                    BroadcastTableInfo currRightTable, MetricsCollector metricsCollector)
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
                    chainJoinSplit(queryId, currJoiner, nextJoiner, inputs, !currRightTable.isBase(),
                            currRightTable.getColumnsToRead(), currRightFilter, metricsCollector);
                }
                catch (Exception e)
                {
                    throw new PixelsWorkerException("error during broadcast join", e);
                }
            }));
        }
        for (Future future : rightFutures)
        {
            future.get();
        }
        logger.info("joined with chain table: " + currRightTable.getTableName());
    }

    /**
     * Perform the join of two left tables on one split of the right one.
     *
     * @param currJoiner the joiner of the two left tables
     * @param nextJoiner the joiner of the next join
     * @param rightInputs the information of the input files in the split of the right one
     *                   of the two left tables
     * @param checkExistence whether check the existence of the input files
     * @param rightCols the column names of the right one of the two left tables
     * @param rightFilter the filter of the right one of the two left tables
     * @param metricsCollector the collector of the performance metrics
     */
    private static void chainJoinSplit(
            long queryId, Joiner currJoiner, Joiner nextJoiner, List<InputInfo> rightInputs, boolean checkExistence,
            String[] rightCols, TableScanFilter rightFilter, MetricsCollector metricsCollector)
    {
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeCostTimer = new MetricsCollector.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightInputs.isEmpty())
        {
            for (Iterator<InputInfo> it = rightInputs.iterator(); it.hasNext(); )
            {
                InputInfo input = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = getReader(input.getPath(), s3))
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
                    throw new PixelsWorkerException("failed to scan the right table input file '" +
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
                    throw new PixelsWorkerException("interrupted while waiting for the input files");
                }
            }
        }
        metricsCollector.addReadBytes(readBytes);
        metricsCollector.addNumReadRequests(numReadRequests);
        metricsCollector.addComputeCostNs(computeCostTimer.getElapsedNs());
        metricsCollector.addInputCostNs(readCostTimer.getElapsedNs());
    }
}
