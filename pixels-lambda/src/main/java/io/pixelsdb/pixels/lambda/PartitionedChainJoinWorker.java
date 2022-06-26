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
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.executor.lambda.domain.BroadcastTableInfo;
import io.pixelsdb.pixels.executor.lambda.domain.ChainJoinInfo;
import io.pixelsdb.pixels.executor.lambda.domain.MultiOutputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.PartitionInfo;
import io.pixelsdb.pixels.executor.lambda.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.executor.lambda.output.JoinOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;
import static io.pixelsdb.pixels.lambda.BroadcastChainJoinWorker.buildFirstJoiner;
import static io.pixelsdb.pixels.lambda.BroadcastChainJoinWorker.chainJoin;
import static io.pixelsdb.pixels.lambda.PartitionedJoinWorker.*;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;
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
    private boolean partitionOutput = false;
    private PartitionInfo outputPartitionInfo;

    @Override
    public JoinOutput handleRequest(PartitionedChainJoinInput event, Context context)
    {
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
            checkArgument(chainTables.size() == chainJoinInfos.size()+1,
                    "left table num is not consistent with (chain-join info num + 1).");
            checkArgument(chainTables.size() > 1, "there should be at least two left tables");

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
            logger.info("small table '" + event.getSmallTable().getTableName() +
                    "', large table '" + event.getLargeTable().getTableName() +
                    "', number of partitions (" + numPartition + ")");

            MultiOutputInfo outputInfo = event.getOutput();
            if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
            {
                checkArgument(rightParallelism + 1 == outputInfo.getFileNames().size(),
                        "the number of output file names is incorrect");
            }
            else
            {
                checkArgument(rightParallelism == outputInfo.getFileNames().size(),
                        "the number of output file names is incorrect");
            }
            String outputFolder = outputInfo.getPath();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = outputInfo.isEncoding();

            this.partitionOutput = event.getJoinInfo().isPostPartition();
            this.outputPartitionInfo = event.getJoinInfo().getPostPartitionInfo();

            if (this.partitionOutput)
            {
                logger.info("post partition num: " + this.outputPartitionInfo.getNumPartition());
            }

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

            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            getFileSchema(threadPool, s3, leftSchema, rightSchema,
                    leftPartitioned.get(0), rightPartitioned.get(0), true);
            Joiner partitionJoiner = new Joiner(joinType,
                    leftSchema.get(), leftColAlias, leftProjection, leftKeyColumnIds,
                    rightSchema.get(), rightColAlias, rightProjection, rightKeyColumnIds);
            // build the chain joiner.
            Joiner chainJoiner = buildChainJoiner(queryId, threadPool, chainTables, chainJoinInfos,
                    partitionJoiner.getJoinedSchema());

            JoinOutput joinOutput = new JoinOutput();
            if (chainJoiner.getSmallTableSize() == 0)
            {
                // the result of the chain joins is empty, no need to continue the join.
                return joinOutput;
            }

            // build the hash table for the left partitioned table.
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
                        buildHashTable(queryId, partitionJoiner, parts, leftCols, hashValues, numPartition);
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
            logger.info("hash table size: " + partitionJoiner.getSmallTableSize());

            if (partitionJoiner.getSmallTableSize() == 0)
            {
                // the left table is empty, no need to continue the join.
                return joinOutput;
            }
            // scan the right table and do the join.
            int rightSplitSize = rightPartitioned.size() / rightParallelism;
            if (rightPartitioned.size() % rightParallelism > 0)
            {
                rightSplitSize++;
            }
            for (int i = 0, outputId = 0; i < rightPartitioned.size(); i += rightSplitSize, ++outputId)
            {
                List<String> parts = new LinkedList<>();
                for (int j = i; j < i + rightSplitSize && j < rightPartitioned.size(); ++j)
                {
                    parts.add(rightPartitioned.get(j));
                }
                String outputPath = outputFolder + outputInfo.getFileNames().get(outputId);
                threadPool.execute(() -> {
                    try
                    {
                        // TODO: deal with the chainJoiner.
                        int rowGroupNum = this.partitionOutput ?
                                joinWithRightTableAndPartition(
                                        queryId, partitionJoiner, parts, rightCols, hashValues,
                                        numPartition, outputPath, encoding, outputInfo.getScheme(),
                                        outputPartitionInfo) :
                                joinWithRightTable(queryId, partitionJoiner, parts, rightCols,
                                hashValues, numPartition, outputPath, encoding, outputInfo.getScheme());
                        if (rowGroupNum > 0)
                        {
                            joinOutput.addOutput(outputPath, rowGroupNum);
                        }
                    }
                    catch (Exception e)
                    {
                        logger.error("error during hash join", e);
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

    private static Joiner buildChainJoiner(long queryId, ExecutorService executor,
                                      List<BroadcastTableInfo> chainTables,
                                      List<ChainJoinInfo> chainJoinInfos,
                                      TypeDescription lastResultSchema)
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
            Joiner currJoiner = buildFirstJoiner(queryId, executor, t1, t2, currChainJoin);
            for (int i = 1; i < chainTables.size() - 1; ++i)
            {
                BroadcastTableInfo currRightTable = chainTables.get(i);
                BroadcastTableInfo nextChainTable = chainTables.get(i+1);
                TypeDescription nextTableSchema = getFileSchema(s3,
                        nextChainTable.getInputSplits().get(0).getInputInfos().get(0).getPath(),
                        true);
                TypeDescription nextResultSchema = getResultSchema(
                        nextTableSchema, nextChainTable.getColumnsToRead());

                ChainJoinInfo nextChainJoin = chainJoinInfos.get(i);
                Joiner nextJoiner = new Joiner(nextChainJoin.getJoinType(),
                        currJoiner.getJoinedSchema(), nextChainJoin.getSmallColumnAlias(),
                        nextChainJoin.getSmallProjection(), currChainJoin.getKeyColumnIds(),
                        nextResultSchema, nextChainJoin.getLargeColumnAlias(),
                        nextChainJoin.getLargeProjection(), nextChainTable.getKeyColumnIds());

                chainJoin(queryId, executor, currJoiner, nextJoiner, currRightTable);
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
            chainJoin(queryId, executor, currJoiner, finalJoiner, lastChainTable);
            return finalJoiner;
        } catch (Exception e)
        {
            throw new RuntimeException("failed to join left tables", e);
        }
    }
}
