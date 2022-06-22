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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;
import static io.pixelsdb.pixels.lambda.BroadcastJoinWorker.*;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;
import static java.util.Objects.requireNonNull;

/**
 * Chain join is the combination of a set of broadcast joins.
 * All the left tables in a chain join are broadcast.
 *
 * @author hank
 * @date 03/06/2022
 */
public class BroadcastChainJoinWorker implements RequestHandler<BroadcastChainJoinInput, JoinOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(BroadcastChainJoinWorker.class);
    private long queryId;
    private boolean partitionOutput = false;
    private PartitionInfo outputPartitionInfo;

    @Override
    public JoinOutput handleRequest(BroadcastChainJoinInput event, Context context)
    {
        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            // String requestId = context.getAwsRequestId();

            this.queryId = event.getQueryId();

            List<BroadCastJoinTableInfo> leftTables = event.getSmallTables();
            List<ChainJoinInfo> chainJoinInfos = event.getChainJoinInfos();
            requireNonNull(leftTables, "leftTables is null");
            requireNonNull(chainJoinInfos, "chainJoinInfos is null");
            checkArgument(leftTables.size() == chainJoinInfos.size()+1,
                    "left table num is not consistent with (chain-join info num + 1).");
            checkArgument(leftTables.size() > 1, "there should be at least two left tables");

            BroadCastJoinTableInfo rightTable = event.getLargeTable();
            List<InputSplit> rightInputs = rightTable.getInputSplits();
            checkArgument(rightInputs.size() > 0, "rightPartitioned is empty");
            String[] rightCols = rightTable.getColumnsToRead();
            TableScanFilter rightFilter = JSON.parseObject(rightTable.getFilter(), TableScanFilter.class);

            JoinInfo lastJoinInfo = event.getJoinInfo();
            JoinType joinType = lastJoinInfo.getJoinType();
            checkArgument(joinType != JoinType.EQUI_LEFT && joinType != JoinType.EQUI_FULL,
                    "broadcast join can not be used for LEFT_OUTER or FULL_OUTER join");

            MultiOutputInfo outputInfo = event.getOutput();
            checkArgument(rightInputs.size() == outputInfo.getFileNames().size(),
                    "the number of output file names is incorrect");
            String outputFolder = outputInfo.getPath();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = outputInfo.isEncoding();

            try
            {
                if (minio == null && outputInfo.getScheme() == Storage.Scheme.minio)
                {
                    ConfigMinIO(outputInfo.getEndpoint(), outputInfo.getAccessKey(), outputInfo.getSecretKey());
                    minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                }
            } catch (Exception e)
            {
                logger.error("failed to initialize MinIO storage", e);
            }

            this.partitionOutput = event.getJoinInfo().isPostPartition();
            this.outputPartitionInfo = event.getJoinInfo().getPostPartitionInfo();

            if (this.partitionOutput)
            {
                logger.info("post partitioning, number of partitions: " + this.outputPartitionInfo.getNumParition());
            }

            // build the joiner.
            Joiner joiner = buildJoiner(threadPool, leftTables, chainJoinInfos, rightTable, lastJoinInfo);
            // scan the right table and do the join.
            JoinOutput joinOutput = new JoinOutput();
            int i = 0;
            for (InputSplit inputSplit : rightInputs)
            {
                List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                String outputPath = outputFolder + outputInfo.getFileNames().get(i++);
                threadPool.execute(() -> {
                    try
                    {
                        int rowGroupNum = this.partitionOutput ?
                                joinWithRightTableAndPartition(
                                        queryId, joiner, inputs, true, rightCols, rightFilter,
                                        outputPath, encoding, outputInfo.getScheme(), this.partitionOutput,
                                        this.outputPartitionInfo) :
                                joinWithRightTable(queryId, joiner, inputs, true, rightCols,
                                        rightFilter, outputPath, encoding, outputInfo.getScheme());
                        if (rowGroupNum > 0)
                        {
                            joinOutput.addOutput(outputPath, rowGroupNum);
                        }
                    }
                    catch (Exception e)
                    {
                        logger.error("error during broadcast join", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                logger.error("interrupted while waiting for the termination of join", e);
            }

            return joinOutput;
        } catch (Exception e)
        {
            logger.error("error during join", e);
            return null;
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
     * @return the joiner of the last join
     */
    private Joiner buildJoiner(ExecutorService executor,
                                  List<BroadCastJoinTableInfo> leftTables,
                                  List<ChainJoinInfo> chainJoinInfos,
                                  BroadCastJoinTableInfo rightTable,
                                  JoinInfo lastJoinInfo)
    {
        try
        {
            BroadCastJoinTableInfo t1 = leftTables.get(0);
            BroadCastJoinTableInfo t2 = leftTables.get(1);
            Joiner currJoiner = buildFirstJoiner(executor, t1, t2, chainJoinInfos.get(0));
            for (int i = 1; i < leftTables.size() - 1; ++i)
            {
                BroadCastJoinTableInfo currRightTable = leftTables.get(i);
                BroadCastJoinTableInfo nextTable = leftTables.get(i+1);
                TypeDescription nextTableSchema = getFileSchema(s3,
                        nextTable.getInputSplits().get(0).getInputInfos().get(0).getPath());
                ChainJoinInfo currJoinInfo = chainJoinInfos.get(i-1);
                ChainJoinInfo nextJoinInfo = chainJoinInfos.get(i);
                TypeDescription nextResultSchema = getResultSchema(nextTableSchema, nextTable.getColumnsToRead());
                Joiner nextJoiner = new Joiner(nextJoinInfo.getJoinType(),
                        currJoiner.getJoinedSchema(), nextJoinInfo.getSmallColumnAlias(),
                        nextJoinInfo.getSmallProjection(), currJoinInfo.getKeyColumnIds(),
                        nextResultSchema, nextJoinInfo.getLargeColumnAlias(),
                        nextJoinInfo.getLargeProjection(), nextTable.getKeyColumnIds());
                chainJoin(executor, currJoiner, nextJoiner, currRightTable);
                currJoiner = nextJoiner;
            }
            ChainJoinInfo lastChainJoin = chainJoinInfos.get(chainJoinInfos.size()-1);
            BroadCastJoinTableInfo lastLeftTable = leftTables.get(leftTables.size()-1);
            TypeDescription rightTableSchema = getFileSchema(s3,
                    rightTable.getInputSplits().get(0).getInputInfos().get(0).getPath());
            TypeDescription rightResultSchema = getResultSchema(rightTableSchema, rightTable.getColumnsToRead());
            Joiner finalJoiner = new Joiner(lastJoinInfo.getJoinType(),
                    currJoiner.getJoinedSchema(), lastJoinInfo.getSmallColumnAlias(),
                    lastJoinInfo.getSmallProjection(), lastChainJoin.getKeyColumnIds(),
                    rightResultSchema, lastJoinInfo.getLargeColumnAlias(),
                    lastJoinInfo.getLargeProjection(), rightTable.getKeyColumnIds());
            chainJoin(executor, currJoiner, finalJoiner, lastLeftTable);
            return finalJoiner;
        } catch (Exception e)
        {
            throw new RuntimeException("failed to join left tables", e);
        }
    }

    /**
     * Build the joiner for the join between the first two left tables.
     *
     * @param executor the thread pool
     * @param t1 the information of the first left table
     * @param t2 the information of the second left table
     * @param joinInfo the information of the join between t1 and t2
     * @return the joiner of the first join
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private Joiner buildFirstJoiner(ExecutorService executor,
                                    BroadCastJoinTableInfo t1,
                                    BroadCastJoinTableInfo t2,
                                    ChainJoinInfo joinInfo) throws ExecutionException, InterruptedException
    {
        AtomicReference<TypeDescription> t1Schema = new AtomicReference<>();
        AtomicReference<TypeDescription> t2Schema = new AtomicReference<>();
        getFileSchema(executor, s3, t1Schema, t2Schema,
                t1.getInputSplits().get(0).getInputInfos().get(0).getPath(),
                t2.getInputSplits().get(0).getInputInfos().get(0).getPath());
        Joiner joiner = new Joiner(joinInfo.getJoinType(),
                getResultSchema(t1Schema.get(), t1.getColumnsToRead()), joinInfo.getSmallColumnAlias(),
                joinInfo.getSmallProjection(), t1.getKeyColumnIds(),
                getResultSchema(t2Schema.get(), t2.getColumnsToRead()), joinInfo.getLargeColumnAlias(),
                joinInfo.getLargeProjection(), t2.getKeyColumnIds());
        List<Future> leftFutures = new ArrayList<>();
        TableScanFilter t1Filter = JSON.parseObject(t1.getFilter(), TableScanFilter.class);
        for (InputSplit inputSplit : t1.getInputSplits())
        {
            List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
            leftFutures.add(executor.submit(() -> {
                try
                {
                    buildHashTable(queryId, joiner, inputs, true, t1.getColumnsToRead(), t1Filter);
                }
                catch (Exception e)
                {
                    logger.error("error during hash table construction", e);
                }
            }));
        }
        for (Future future : leftFutures)
        {
            future.get();
        }
        logger.info("first left table: " + t1.getTableName() + ", hash table size: " + joiner.getSmallTableSize());
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
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void chainJoin(ExecutorService executor, Joiner currJoiner, Joiner nextJoiner,
                           BroadCastJoinTableInfo currRightTable) throws ExecutionException, InterruptedException
    {
        TableScanFilter currRigthFilter = JSON.parseObject(currRightTable.getFilter(), TableScanFilter.class);
        List<Future> rightFutures = new ArrayList<>();
        for (InputSplit inputSplit : currRightTable.getInputSplits())
        {
            List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
            rightFutures.add(executor.submit(() -> {
                try
                {
                    chainJoinSplit(currJoiner, nextJoiner, inputs, true,
                            currRightTable.getColumnsToRead(), currRigthFilter);
                }
                catch (Exception e)
                {
                    logger.error("error during broadcast join", e);
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
     */
    private void chainJoinSplit(Joiner currJoiner, Joiner nextJoiner, List<InputInfo> rightInputs,
                                boolean checkExistence, String[] rightCols, TableScanFilter rightFilter)
    {
        while (!rightInputs.isEmpty())
        {
            for (Iterator<InputInfo> it = rightInputs.iterator(); it.hasNext(); )
            {
                InputInfo input = it.next();
                if (checkExistence)
                {
                    long start = System.currentTimeMillis();
                    try
                    {
                        if (s3.exists(input.getPath()))
                        {
                            it.remove();
                        } else
                        {
                            continue;
                        }
                    } catch (IOException e)
                    {
                        logger.error("failed to check the existence of the right table input file '" +
                                input.getPath() + "'", e);
                    }
                    long end = System.currentTimeMillis();
                    logger.info("duration of existence check: " + (end - start));
                }
                else
                {
                    it.remove();
                }
                try (PixelsReader pixelsReader = getReader(input.getPath(), s3))
                {
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
                    int scannedRows = 0, joinedRows = 0;
                    Bitmap filtered = new Bitmap(rowBatchSize, true);
                    Bitmap tmp = new Bitmap(rowBatchSize, false);
                    do
                    {
                        rowBatch = recordReader.readBatch(rowBatchSize);
                        rightFilter.doFilter(rowBatch, filtered, tmp);
                        rowBatch.applyFilter(filtered);
                        scannedRows += rowBatch.size;
                        if (rowBatch.size > 0)
                        {
                            List<VectorizedRowBatch> joinedBatches = currJoiner.join(rowBatch);
                            for (VectorizedRowBatch joined : joinedBatches)
                            {
                                if (!joined.isEmpty())
                                {
                                    nextJoiner.populateLeftTable(joined);
                                    joinedRows += joined.size;
                                }
                            }
                        }
                    } while (!rowBatch.endOfFile);
                    logger.info("number of scanned rows: " + scannedRows +
                            ", number of joined rows: " + joinedRows);
                } catch (Exception e)
                {
                    logger.error("failed to scan the right table input file '" +
                            input.getPath() + "' and do the join", e);
                }
            }
        }
    }
}
