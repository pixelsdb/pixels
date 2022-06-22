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
import io.pixelsdb.pixels.executor.lambda.input.PartitionedJoinInput;
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
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 07/05/2022
 */
public class PartitionedJoinWorker implements RequestHandler<PartitionedJoinInput, JoinOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionedJoinWorker.class);
    private boolean partitionOutput = false;
    private PartitionInfo outputPartitionInfo;

    @Override
    public JoinOutput handleRequest(PartitionedJoinInput event, Context context)
    {
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
                    leftPartitioned.get(0), rightPartitioned.get(0));
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
                        buildHashTable(queryId, joiner, parts, leftCols, hashValues, numPartition);
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
            logger.info("hash table size: " + joiner.getSmallTableSize());
            JoinOutput joinOutput = new JoinOutput();
            if (joiner.getSmallTableSize() == 0)
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
                        int rowGroupNum = this.partitionOutput ?
                                joinWithRightTableAndPartition(
                                        queryId, joiner, parts, rightCols, hashValues,
                                        numPartition, outputPath, encoding, outputInfo.getScheme()) :
                                joinWithRightTable(queryId, joiner, parts, rightCols,
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

            if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
            {
                // output the left-outer tail.
                String outputPath = outputFolder + outputInfo.getFileNames().get(
                        outputInfo.getFileNames().size()-1);
                PixelsWriter pixelsWriter;
                if (partitionOutput)
                {
                    requireNonNull(this.outputPartitionInfo, "outputPartitionInfo is null");
                    pixelsWriter = getWriter(joiner.getJoinedSchema(),
                            outputInfo.getScheme() == Storage.Scheme.minio ? minio : s3, outputPath,
                            encoding, true, Arrays.stream(
                                    this.outputPartitionInfo.getKeyColumnIds()).boxed().
                                    collect(Collectors.toList()));
                    joiner.writeLeftOuterAndPartition(pixelsWriter, rowBatchSize, outputPartitionInfo);
                }
                else
                {
                    pixelsWriter = getWriter(joiner.getJoinedSchema(),
                            outputInfo.getScheme() == Storage.Scheme.minio ? minio : s3, outputPath,
                            encoding, false, null);
                    joiner.writeLeftOuter(pixelsWriter, rowBatchSize);
                }
                pixelsWriter.close();
                joinOutput.addOutput(outputPath, pixelsWriter.getRowGroupNum());
            }

            return joinOutput;
        } catch (Exception e)
        {
            logger.error("error during join", e);
            return null;
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
     */
    private void buildHashTable(long queryId, Joiner joiner, List<String> leftParts,
                               String[] leftCols, List<Integer> hashValues, int numPartition)
    {
        while (!leftParts.isEmpty())
        {
            for (Iterator<String> it = leftParts.iterator(); it.hasNext(); )
            {
                String leftPartitioned = it.next();
                try
                {
                    if (s3.exists(leftPartitioned))
                    {
                        it.remove();
                    } else
                    {
                        continue;
                    }
                } catch (IOException e)
                {
                    logger.error("failed to check the existence of the partitioned file '" +
                            leftPartitioned + "' of the left table", e);
                }
                try (PixelsReader pixelsReader = getReader(leftPartitioned, s3))
                {
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
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");
                        do
                        {
                            rowBatch = recordReader.readBatch(rowBatchSize);
                            if (rowBatch.size > 0)
                            {
                                joiner.populateLeftTable(rowBatch);
                            }
                        } while (!rowBatch.endOfFile);
                    }
                } catch (Exception e)
                {
                    logger.error("failed to scan the partitioned file '" +
                            leftPartitioned + "' and build the hash table", e);
                }
            }
        }
    }

    /**
     * Scan the partitioned file of the right table and do the join.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for which the hash table is built
     * @param rightParts the information of partitioned files of the right table
     * @param rightCols the column names of the right table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param outputPath fileName on s3 to store the scan results
     * @param encoding whether encode the scan results or not
     * @param outputScheme the storage scheme of the output files
     * @return the number of row groups that have been written into the output.
     */
    private int joinWithRightTable(long queryId, Joiner joiner, List<String> rightParts,
                                   String[] rightCols, List<Integer> hashValues, int numPartition,
                                   String outputPath, boolean encoding, Storage.Scheme outputScheme)
    {
        PixelsWriter pixelsWriter = getWriter(joiner.getJoinedSchema(),
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
                    logger.error("failed to check the existence of the partitioned file '" +
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
                        VectorizedRowBatch rowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");
                        int scannedRows = 0, joinedRows = 0;
                        do
                        {
                            rowBatch = recordReader.readBatch(rowBatchSize);
                            scannedRows += rowBatch.size;
                            if (rowBatch.size > 0)
                            {
                                List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                                for (VectorizedRowBatch joined : joinedBatches)
                                {
                                    if (!joined.isEmpty())
                                    {
                                        pixelsWriter.addRowBatch(joined);
                                        joinedRows += joined.size;
                                    }
                                }
                            }
                        } while (!rowBatch.endOfFile);
                        logger.info("number of scanned rows: " + scannedRows +
                                ", number of joined rows: " + joinedRows);
                    }
                } catch (Exception e)
                {
                    logger.error("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
                }
            }
        }
        try
        {
            pixelsWriter.close();
            if (outputScheme == Storage.Scheme.minio)
            {
                while (true)
                {
                    try
                    {
                        if (minio.getStatus(outputPath) != null)
                        {
                            break;
                        }
                    } catch (Exception e)
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }
            }
        } catch (Exception e)
        {
            logger.error("failed to finish writing and close the join result file '" + outputPath + "'", e);
        }
        return pixelsWriter.getRowGroupNum();
    }

    /**
     * Scan the partitioned file of the right table, do the join, and partition the output.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for which the hash table is built
     * @param rightParts the information of partitioned files of the right table
     * @param rightCols the column names of the right table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param outputPath fileName on s3 to store the scan results
     * @param encoding whether encode the scan results or not
     * @param outputScheme the storage scheme of the output files
     * @return the number of row groups that have been written into the output.
     */
    private int joinWithRightTableAndPartition(long queryId, Joiner joiner, List<String> rightParts,
                                               String[] rightCols, List<Integer> hashValues,
                                               int numPartition, String outputPath, boolean encoding,
                                               Storage.Scheme outputScheme)
    {
        checkArgument(this.partitionOutput, "partitionOutput is false");
        requireNonNull(this.outputPartitionInfo, "outputPartitionInfo is null");
        Partitioner partitioner = new Partitioner(this.outputPartitionInfo.getNumParition(),
                rowBatchSize, joiner.getJoinedSchema(), outputPartitionInfo.getKeyColumnIds());
        List<List<VectorizedRowBatch>> partitioned = new ArrayList<>(outputPartitionInfo.getNumParition());
        for (int i = 0; i < outputPartitionInfo.getNumParition(); ++i)
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
                    logger.error("failed to check the existence of the partitioned file '" +
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
                        VectorizedRowBatch rowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");
                        int scannedRows = 0, joinedRows = 0;
                        do
                        {
                            rowBatch = recordReader.readBatch(rowBatchSize);
                            scannedRows += rowBatch.size;
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
                                            partitioned.get(entry.getKey()).add(entry.getValue());
                                        }
                                        joinedRows += joined.size;
                                    }
                                }
                            }
                        } while (!rowBatch.endOfFile);
                        logger.info("number of scanned rows: " + scannedRows +
                                ", number of joined rows: " + joinedRows);
                    }
                } catch (Exception e)
                {
                    logger.error("failed to scan the partitioned file '" +
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
            PixelsWriter pixelsWriter = getWriter(joiner.getJoinedSchema(),
                    outputScheme == Storage.Scheme.minio ? minio : s3, outputPath,
                    encoding, true, Arrays.stream(
                            this.outputPartitionInfo.getKeyColumnIds()).boxed().
                            collect(Collectors.toList()));
            int rowNum = 0;
            for (int hash = 0; hash < outputPartitionInfo.getNumParition(); ++hash)
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
                while (true)
                {
                    try
                    {
                        if (minio.getStatus(outputPath) != null)
                        {
                            break;
                        }
                    } catch (Exception e)
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }
            }
        } catch (Exception e)
        {
            logger.error("failed to finish writing and close the join result file '" + outputPath + "'", e);
        }
        return rowGroupNum;
    }

    /**
     * Create the reader option for a record reader of the given hash partition in a partitioned file.
     * It must be checked outside that the given hash partition info exists in the file.
     * @param queryId the query id
     * @param cols the column names in the partitioned file
     * @param pixelsReader the reader of the partitioned file
     * @param hashValue the hash value of the given hash partition
     * @param numPartition the total number of partitions
     * @return the reader option
     */
    private PixelsReaderOption getReaderOption(long queryId, String[] cols, PixelsReader pixelsReader,
                                               int hashValue, int numPartition)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.queryId(queryId);
        option.includeCols(cols);
        if (pixelsReader.getRowGroupNum() == numPartition)
        {
            option.rgRange(hashValue, 1);
        } else
        {
            for (int i = 0; i < pixelsReader.getRowGroupNum(); ++i)
            {
                PixelsProto.RowGroupInformation info = pixelsReader.getRowGroupInfo(i);
                if (info.getPartitionInfo().getHashValue() == hashValue)
                {
                    option.rgRange(hashValue, 1);
                    break;
                }
            }
        }
        return option;
    }
}
