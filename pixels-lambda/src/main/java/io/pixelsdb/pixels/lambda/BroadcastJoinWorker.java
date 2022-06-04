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
public class BroadcastJoinWorker implements RequestHandler<BroadcastJoinInput, JoinOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(BroadcastJoinWorker.class);
    private boolean partitionOutput = false;
    private PartitionInfo outputPartitionInfo;

    @Override
    public JoinOutput handleRequest(BroadcastJoinInput event, Context context)
    {
        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            // String requestId = context.getAwsRequestId();

            long queryId = event.getQueryId();

            BroadCastJoinTableInfo leftTable = event.getLeftTable();
            List<InputSplit> leftInputs = leftTable.getInputSplits();
            requireNonNull(leftInputs, "leftInputs is null");
            checkArgument(leftInputs.size() > 0, "left table is empty");
            String[] leftCols = leftTable.getColumnsToRead();
            int[] leftKeyColumnIds = leftTable.getKeyColumnIds();
            TableScanFilter leftFilter = JSON.parseObject(leftTable.getFilter(), TableScanFilter.class);

            BroadCastJoinTableInfo rightTable = event.getRightTable();
            List<InputSplit> rightInputs = rightTable.getInputSplits();
            requireNonNull(rightInputs, "rightInputs is null");
            checkArgument(rightInputs.size() > 0, "right table is empty");
            String[] rightCols = rightTable.getColumnsToRead();
            int[] rightKeyColumnIds = rightTable.getKeyColumnIds();
            TableScanFilter rightFilter = JSON.parseObject(rightTable.getFilter(), TableScanFilter.class);

            String[] joinedCols = event.getJoinInfo().getResultColumns();
            boolean includeKeyCols = event.getJoinInfo().isOutputJoinKeys();
            JoinType joinType = event.getJoinInfo().getJoinType();

            MultiOutputInfo outputInfo = event.getOutput();
            if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
            {
                checkArgument(rightInputs.size() + 1 == outputInfo.getFileNames().size(),
                        "the number of output file names is incorrect");
            }
            else
            {
                checkArgument(rightInputs.size() == outputInfo.getFileNames().size(),
                        "the number of output file names is incorrect");
            }
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

            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            getFileSchema(threadPool, s3, leftSchema, rightSchema,
                    leftInputs.get(0).getInputInfos().get(0).getPath(),
                    rightInputs.get(0).getInputInfos().get(0).getPath());
            Joiner joiner = new Joiner(joinType, joinedCols, includeKeyCols,
                    getResultSchema(leftSchema.get(), leftCols), leftKeyColumnIds,
                    getResultSchema(rightSchema.get(), rightCols), rightKeyColumnIds);
            // build the hash table for the left table.
            List<Future> leftFutures = new ArrayList<>();
            for (InputSplit inputSplit : leftInputs)
            {
                List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                leftFutures.add(threadPool.submit(() -> {
                    try
                    {
                        buildHashTable(queryId, joiner, inputs, true, leftCols, leftFilter);
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
            logger.info("hash table size: " + joiner.getLeftTableSize());
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
                                joinWithRightTable(queryId, joiner, inputs, false, rightCols,
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
     * Scan the input files of the left table and populate the hash table for the join.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for which the hash table is built
     * @param leftInputs the information of input files of the left table,
     *                   the list <b>must be mutable</b>
     * @param checkExistence whether check the existence of the input files
     * @param leftCols the column names of the left table
     * @param leftFilter the table scan filter on the left table
     */
    public static void buildHashTable(long queryId, Joiner joiner, List<InputInfo> leftInputs,
                                      boolean checkExistence, String[] leftCols, TableScanFilter leftFilter)
    {
        while (!leftInputs.isEmpty())
        {
            for (Iterator<InputInfo> it = leftInputs.iterator(); it.hasNext(); )
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
                        logger.error("failed to check the existence of the left table input file '" +
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
                    PixelsReaderOption option = getReaderOption(queryId, leftCols, input);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");
                    Bitmap filtered = new Bitmap(rowBatchSize, true);
                    Bitmap tmp = new Bitmap(rowBatchSize, false);
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
                } catch (Exception e)
                {
                    logger.error("failed to scan the left table input file '" +
                            input.getPath() + "' and build the hash table", e);
                }
            }
        }
    }

    /**
     * Scan the input files of the right table and do the join.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for which the hash table is built
     * @param rightInputs the information of input files of the right table,
     *                    the list <b>must be mutable</b>
     * @param checkExistence whether check the existence of the input files
     * @param rightCols the column names of the right table
     * @param rightFilter the table scan filter on the right table
     * @param outputPath fileName on s3 to store the scan results
     * @param encoding whether encode the scan results or not
     * @param outputScheme the storage scheme of the output files
     * @return the number of row groups that have been written into the output.
     */
    public static int joinWithRightTable(long queryId, Joiner joiner, List<InputInfo> rightInputs,
                                   boolean checkExistence, String[] rightCols, TableScanFilter rightFilter,
                                   String outputPath, boolean encoding, Storage.Scheme outputScheme)
    {
        PixelsWriter pixelsWriter = getWriter(joiner.getJoinedSchema(),
                outputScheme == Storage.Scheme.minio ? minio : s3, outputPath,
                encoding, false, null);
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
                } catch (Exception e)
                {
                    logger.error("failed to scan the right table input file '" +
                            input.getPath() + "' and do the join", e);
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
     * Scan the input files of the right table, do the join, and partition the result.
     *
     * @param queryId the query id used by I/O scheduler
     * @param joiner the joiner for which the hash table is built
     * @param rightInputs the information of input files of the right table,
     *                    the list <b>must be mutable</b>
     * @param checkExistence whether check the existence of the input files
     * @param rightCols the column names of the right table
     * @param rightFilter the table scan filter on the right table
     * @param outputPath fileName on s3 to store the scan results
     * @param encoding whether encode the scan results or not
     * @param outputScheme the storage scheme of the output files
     * @return the number of row groups that have been written into the output.
     */
    public static int joinWithRightTableAndPartition(
            long queryId, Joiner joiner, List<InputInfo> rightInputs, boolean checkExistence, String[] rightCols,
            TableScanFilter rightFilter, String outputPath, boolean encoding, Storage.Scheme outputScheme,
            boolean partitionOutput, PartitionInfo outputPartitionInfo)
    {
        checkArgument(partitionOutput, "partitionOutput is false");
        requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
        Partitioner partitioner = new Partitioner(outputPartitionInfo.getNumParition(),
                rowBatchSize, joiner.getJoinedSchema(), outputPartitionInfo.getKeyColumnIds());
        List<List<VectorizedRowBatch>> partitioned = new ArrayList<>(outputPartitionInfo.getNumParition());
        for (int i = 0; i < outputPartitionInfo.getNumParition(); ++i)
        {
            partitioned.add(new LinkedList<>());
        }
        int rowGroupNum = 0;
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
                } catch (Exception e)
                {
                    logger.error("failed to scan the right table input file '" +
                            input.getPath() + "' and do the join", e);
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
                            outputPartitionInfo.getKeyColumnIds()).boxed().
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
}
