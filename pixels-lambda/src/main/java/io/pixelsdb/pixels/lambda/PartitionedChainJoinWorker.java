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
import io.pixelsdb.pixels.executor.lambda.domain.BroadcastTableInfo;
import io.pixelsdb.pixels.executor.lambda.domain.ChainJoinInfo;
import io.pixelsdb.pixels.executor.lambda.domain.MultiOutputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.PartitionInfo;
import io.pixelsdb.pixels.executor.lambda.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.executor.lambda.output.JoinOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;
import static io.pixelsdb.pixels.lambda.BroadcastChainJoinWorker.buildFirstJoiner;
import static io.pixelsdb.pixels.lambda.BroadcastChainJoinWorker.chainJoin;
import static io.pixelsdb.pixels.lambda.PartitionedJoinWorker.buildHashTable;
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

    @Override
    public JoinOutput handleRequest(PartitionedChainJoinInput event, Context context)
    {
        JoinOutput joinOutput = new JoinOutput();
        long startTime = System.currentTimeMillis();
        joinOutput.setStartTimeMs(startTime);
        joinOutput.setRequestId(context.getAwsRequestId());
        joinOutput.setSuccessful(true);
        joinOutput.setErrorMessage("");

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

            ChainJoinInfo lastJoin = chainJoinInfos.get(chainJoinInfos.size()-1);
            boolean partitionOutput = lastJoin.isPostPartition();
            PartitionInfo outputPartitionInfo = lastJoin.getPostPartitionInfo();

            if (partitionOutput)
            {
                logger.info("post partition num: " + outputPartitionInfo.getNumPartition());
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
                throw new PixelsWorkerException("failed to initialize MinIO storage", e);
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

            if (chainJoiner.getSmallTableSize() == 0)
            {
                // the result of the chain joins is empty, no need to continue the join.
                joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
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
                        throw new PixelsWorkerException("error during hash table construction", e);
                    }
                }));
            }
            for (Future future : leftFutures)
            {
                future.get();
            }
            logger.info("hash table size of small partitioned table '" +
                    event.getSmallTable().getTableName() + "': " + partitionJoiner.getSmallTableSize());

            if (partitionJoiner.getSmallTableSize() == 0)
            {
                // the left table is empty, no need to continue the join.
                joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
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
                        int rowGroupNum = partitionOutput ?
                                joinWithRightTableAndPartition(
                                        queryId, partitionJoiner, chainJoiner, parts, rightCols, hashValues,
                                        numPartition, outputPath, encoding, outputInfo.getScheme(),
                                        outputPartitionInfo) :
                                joinWithRightTable(queryId, partitionJoiner, chainJoiner, parts, rightCols,
                                hashValues, numPartition, outputPath, encoding, outputInfo.getScheme());
                        if (rowGroupNum > 0)
                        {
                            joinOutput.addOutput(outputPath, rowGroupNum);
                        }
                    }
                    catch (Exception e)
                    {
                        throw new PixelsWorkerException("error during hash join", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                throw new PixelsWorkerException("interrupted while waiting for the termination of join", e);
            }

            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
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
     * @param outputPath fileName on s3 to store the scan results
     * @param encoding whether encode the scan results or not
     * @param outputScheme the storage scheme of the output files
     * @return the number of row groups that have been written into the output.
     */
    protected static int joinWithRightTable(long queryId,
                                            Joiner partitionedJoiner, Joiner chainJoiner,
                                            List<String> rightParts, String[] rightCols,
                                            List<Integer> hashValues, int numPartition,
                                            String outputPath, boolean encoding,
                                            Storage.Scheme outputScheme)
    {
        PixelsWriter pixelsWriter = getWriter(chainJoiner.getJoinedSchema(),
                outputScheme == Storage.Scheme.minio ? minio : s3, outputPath,
                encoding, false, null);
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                try
                {
                    if (s3.exists(rightPartitioned))
                    {
                        it.remove();
                    } else
                    {
                        continue;
                    }
                } catch (IOException e)
                {
                    throw new PixelsWorkerException("failed to check the existence of the partitioned file '" +
                            rightPartitioned + "' of the right table", e);
                }

                try (PixelsReader pixelsReader = getReader(rightPartitioned, s3))
                {
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
                        VectorizedRowBatch rightRowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");
                        int scannedRows = 0, joinedRows = 0;
                        do
                        {
                            rightRowBatch = recordReader.readBatch(rowBatchSize);
                            scannedRows += rightRowBatch.size;
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
                                                pixelsWriter.addRowBatch(chainJoinResult);
                                                joinedRows += chainJoinResult.size;
                                            }
                                        }
                                    }
                                }
                            }
                        } while (!rightRowBatch.endOfFile);
                        logger.info("number of rows scanned from right partitioned file: " +
                                scannedRows + ", number of final joined rows: " + joinedRows);
                    }
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
                }
            }
        }

        try
        {
            pixelsWriter.close();
            if (outputScheme == Storage.Scheme.minio)
            {
                while (!minio.exists(outputPath))
                {
                    // Wait for 10ms and see if the output file is visible.
                    TimeUnit.MILLISECONDS.sleep(10);
                }
            }
        } catch (Exception e)
        {
            throw new PixelsWorkerException(
                    "failed to finish writing and close the join result file '" + outputPath + "'", e);
        }
        return pixelsWriter.getRowGroupNum();
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
     * @param outputPath fileName on s3 to store the scan results
     * @param encoding whether encode the scan results or not
     * @param outputScheme the storage scheme of the output files
     * @param postPartitionInfo the partition information of post partitioning
     * @return the number of row groups that have been written into the output.
     */
    protected static int joinWithRightTableAndPartition(long queryId,
                                                        Joiner partitionedJoiner, Joiner chainJoiner,
                                                        List<String> rightParts, String[] rightCols,
                                                        List<Integer> hashValues, int numPartition,
                                                        String outputPath, boolean encoding,
                                                        Storage.Scheme outputScheme,
                                                        PartitionInfo postPartitionInfo)
    {
        requireNonNull(postPartitionInfo, "outputPartitionInfo is null");
        Partitioner partitioner = new Partitioner(postPartitionInfo.getNumPartition(),
                rowBatchSize, chainJoiner.getJoinedSchema(), postPartitionInfo.getKeyColumnIds());
        List<List<VectorizedRowBatch>> partitioned = new ArrayList<>(postPartitionInfo.getNumPartition());
        for (int i = 0; i < postPartitionInfo.getNumPartition(); ++i)
        {
            partitioned.add(new LinkedList<>());
        }
        int rowGroupNum = 0;
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                try
                {
                    if (s3.exists(rightPartitioned))
                    {
                        it.remove();
                    } else
                    {
                        continue;
                    }
                } catch (IOException e)
                {
                    throw new PixelsWorkerException("failed to check the existence of the partitioned file '" +
                            rightPartitioned + "' of the right table", e);
                }

                try (PixelsReader pixelsReader = getReader(rightPartitioned, s3))
                {
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
                        VectorizedRowBatch rightBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");
                        int scannedRows = 0, joinedRows = 0;
                        do
                        {
                            rightBatch = recordReader.readBatch(rowBatchSize);
                            scannedRows += rightBatch.size;
                            if (rightBatch.size > 0)
                            {
                                List<VectorizedRowBatch> partitionedJoinResults =
                                        partitionedJoiner.join(rightBatch);
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
                                                    partitioned.get(entry.getKey()).add(entry.getValue());
                                                }
                                                joinedRows += chainJoinResult.size;
                                            }
                                        }

                                    }
                                }
                            }
                        } while (!rightBatch.endOfFile);
                        logger.info("number of rows scanned from right partitioned file: " +
                                scannedRows + ", number of final joined rows: " + joinedRows);
                    }
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
                }
            }
        }

        try
        {
            VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
            for (int hash = 0; hash < tailBatches.length; ++hash)
            {
                if (!tailBatches[hash].isEmpty())
                {
                    partitioned.get(hash).add(tailBatches[hash]);
                }
            }
            PixelsWriter pixelsWriter = getWriter(chainJoiner.getJoinedSchema(),
                    outputScheme == Storage.Scheme.minio ? minio : s3, outputPath,
                    encoding, true, Arrays.stream(
                            postPartitionInfo.getKeyColumnIds()).boxed().
                            collect(Collectors.toList()));
            int rowNum = 0;
            for (int hash = 0; hash < postPartitionInfo.getNumPartition(); ++hash)
            {
                List<VectorizedRowBatch> batches = partitioned.get(hash);
                if (!batches.isEmpty())
                {
                    for (VectorizedRowBatch batch : batches)
                    {
                        pixelsWriter.addRowBatch(batch, hash);
                        rowNum += batch.size;
                    }
                }
            }
            logger.info("number of partitioned rows: " + rowNum);
            pixelsWriter.close();
            rowGroupNum = pixelsWriter.getRowGroupNum();
            if (outputScheme == Storage.Scheme.minio)
            {
                while (!minio.exists(outputPath))
                {
                    // Wait for 10ms and see if the output file is visible.
                    TimeUnit.MILLISECONDS.sleep(10);
                }
            }
        } catch (Exception e)
        {
            throw new PixelsWorkerException(
                    "failed to finish writing and close the join result file '" + outputPath + "'", e);
        }
        return rowGroupNum;
    }
}
