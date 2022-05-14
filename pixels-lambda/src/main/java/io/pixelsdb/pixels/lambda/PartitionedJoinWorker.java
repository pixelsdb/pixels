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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.executor.lambda.JoinOutput;
import io.pixelsdb.pixels.executor.lambda.PartitionOutput;
import io.pixelsdb.pixels.executor.lambda.PartitionedJoinInput;
import io.pixelsdb.pixels.executor.lambda.ScanInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;

/**
 * @author hank
 * @date 07/05/2022
 */
public class PartitionedJoinWorker implements RequestHandler<PartitionedJoinInput, JoinOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionedJoinWorker.class);
    private static final PixelsFooterCache footerCache = new PixelsFooterCache();
    private static final ConfigFactory configFactory = ConfigFactory.Instance();
    private static final int rowBatchSize;
    private static final int pixelStride;
    private static final int rowGroupSize;
    private static Storage s3;
    private static Storage minio;

    static
    {
        rowBatchSize = Integer.parseInt(configFactory.getProperty("row.batch.size"));
        pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size"));

        try
        {
            s3 = StorageFactory.Instance().getStorage(Storage.Scheme.s3);

        } catch (Exception e)
        {
            logger.error("failed to initialize AWS S3 storage", e);
        }
    }

    @Override
    public JoinOutput handleRequest(PartitionedJoinInput event, Context context)
    {
        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            String requestId = context.getAwsRequestId();

            long queryId = event.getQueryId();

            String leftPrefix = event.getLeftTableName() + ".";
            List<PartitionOutput> leftPartitioned = event.getLeftPartitioned();
            checkArgument(leftPartitioned.size() > 0, "leftPartitioned is empty");
            String[] leftCols = event.getLeftCols();
            int[] leftKeyColumnIds = event.getLeftKeyColumnIds();

            String rightPrefix = event.getRightTableName() + ".";
            List<PartitionOutput> rightPartitioned = event.getRightPartitioned();
            checkArgument(rightPartitioned.size() > 0, "rightPartitioned is empty");
            String[] rightCols = event.getRightCols();
            int[] rightKeyColumnIds = event.getRightKeyColumnIds();

            PartitionedJoinInput.JoinInfo joinInfo = event.getJoinInfo();
            ScanInput.OutputInfo outputInfo = event.getOutput();
            String outputFolder = outputInfo.getFolder();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = outputInfo.isEncoding();
            try
            {
                if (minio == null)
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
            getSchema(threadPool, leftSchema, rightSchema,
                    leftPartitioned.get(0).getPath(), rightPartitioned.get(0).getPath());
            Joiner joiner = new Joiner(joinInfo.getJoinType(),
                    leftPrefix, leftSchema.get(), leftKeyColumnIds,
                    rightPrefix, rightSchema.get(), rightKeyColumnIds);
            // build the hash table for the left table.
            List<Future> leftFutures = new ArrayList<>(leftPartitioned.size());
            int leftSplitSize = leftPartitioned.size() / (cores * 2);
            if (leftSplitSize == 0)
            {
                leftSplitSize = 1;
            }
            for (int i = 0; i < leftPartitioned.size(); i += leftSplitSize)
            {
                List<PartitionOutput> parts = new ArrayList<>(leftSplitSize);
                for (int j = i; j < i + leftSplitSize && j < leftPartitioned.size(); ++j)
                {
                    parts.add(leftPartitioned.get(i));
                }
                leftFutures.add(threadPool.submit(() -> {
                    try
                    {
                        buildHashTable(queryId, joiner, parts, leftCols,
                                joinInfo.getHashValues(), joinInfo.getNumPartition());
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
            int rightSplitSize = rightPartitioned.size() / (cores * 2);
            if (rightSplitSize == 0)
            {
                rightSplitSize = 1;
            }
            for (int i = 0; i < rightPartitioned.size(); i += rightSplitSize)
            {
                List<PartitionOutput> parts = new ArrayList<>(rightSplitSize);
                for (int j = i; j < i + rightSplitSize && j < rightPartitioned.size(); ++j)
                {
                    parts.add(rightPartitioned.get(i));
                }
                String outputPath = outputFolder + requestId + "_join_" + i;
                threadPool.execute(() -> {
                    try
                    {
                        int rowGroupNum = joinWithRightTable(queryId, joiner, parts, rightCols,
                                joinInfo.getHashValues(), joinInfo.getNumPartition(),
                                outputPath, encoding);
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
                logger.error("interrupted while waiting for the termination of scan", e);
            }

            if (joinInfo.getJoinType() == JoinType.EQUI_LEFT)
            {
                // output the left-outer tail.
                String outputPath = outputFolder + requestId + "_join_left_outer";
                PixelsWriter pixelsWriter = getWriter(joiner.getJoinedSchema(), outputPath, encoding);
                joiner.writeLeftOuter(pixelsWriter, rowBatchSize);
                pixelsWriter.close();
                joinOutput.addOutput(outputPath, pixelsWriter.getRowGroupNum());
            }

            return joinOutput;
        } catch (Exception e)
        {
            logger.error("error during scan", e);
            return null;
        }
    }

    private void getSchema(ExecutorService executor, AtomicReference<TypeDescription> leftSchema,
                           AtomicReference<TypeDescription> rightSchema, String leftPath, String rightPath)
    {
        Future<?> leftFuture = executor.submit(() -> {
            try
            {
                leftSchema.set(getReader(leftPath).getFileSchema());
            } catch (IOException e)
            {
                logger.error("failed to read the schema of the left table");
            }
        });
        Future<?> rightFuture = executor.submit(() -> {
            try
            {
                rightSchema.set(getReader(rightPath).getFileSchema());
            } catch (IOException e)
            {
                logger.error("failed to read the schema of the right table");
            }
        });
        try
        {
            leftFuture.get();
            rightFuture.get();
        } catch (Exception e)
        {
            logger.error("interrupted while waiting for the termination of schema read", e);
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
    private void buildHashTable(long queryId, Joiner joiner, List<PartitionOutput> leftParts,
                               String[] leftCols, List<Integer> hashValues, int numPartition)
    {
        for (PartitionOutput leftPartitioned : leftParts)
        {
            try (PixelsReader pixelsReader = getReader(leftPartitioned.getPath()))
            {
                checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                checkArgument(leftPartitioned.getHashValues().size() == pixelsReader.getRowGroupNum(),
                        "the number of partitions (%s) in the pixels file is not correct (%s)",
                        pixelsReader.getRowGroupNum(), leftPartitioned.getHashValues().size());
                for (int hashValue : hashValues)
                {
                    if (!leftPartitioned.getHashValues().contains(hashValue))
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
                        leftPartitioned.getPath() + "' and build the hash table", e);
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
     * @return the number of row groups that have been written into the output.
     */
    private int joinWithRightTable(long queryId, Joiner joiner, List<PartitionOutput> rightParts,
                                    String[] rightCols, List<Integer> hashValues, int numPartition,
                                    String outputPath, boolean encoding)
    {
        PixelsWriter pixelsWriter = getWriter(joiner.getJoinedSchema(), outputPath, encoding);
        for (PartitionOutput rightPartitioned : rightParts)
        {
            try (PixelsReader pixelsReader = getReader(rightPartitioned.getPath()))
            {
                checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                checkArgument(rightPartitioned.getHashValues().size() == pixelsReader.getRowGroupNum(),
                        "the number of partitions (%s) in the pixels file is not correct (%s)",
                        pixelsReader.getRowGroupNum(), rightPartitioned.getHashValues().size());
                for (int hashValue : hashValues)
                {
                    if (!rightPartitioned.getHashValues().contains(hashValue))
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
                            VectorizedRowBatch joined = joiner.join(rowBatch);
                            if (!joined.isEmpty())
                            {
                                pixelsWriter.addRowBatch(joined);
                                joinedRows += joined.size;
                            }
                        }
                    } while (!rowBatch.endOfFile);
                    logger.info("number of scanned rows: " + scannedRows +
                            ", number of joined rows: " + joinedRows);
                }
            } catch (Exception e)
            {
                logger.error("failed to scan the partitioned file '" +
                        rightPartitioned.getPath() + "' and do the join", e);
            }
        }
        try
        {
            pixelsWriter.close();
            while (true)
            {
                try
                {
                    if (minio.getStatus(outputPath) != null)
                    {
                        break;
                    }
                }
                catch (Exception e)
                {
                    // Wait for 10ms and see if the output file is visible.
                    TimeUnit.MILLISECONDS.sleep(10);
                }
            }
        } catch (Exception e)
        {
            logger.error("failed to finish writing and close the join result file '" + outputPath + "'", e);
        }
        return pixelsWriter.getRowGroupNum();
    }

    /**
     * Create the reader option for a record reader of the given hash partition in a partitioned file.
     * It must be checked outside that the given hash partition info exists in the file.
     * @param queryId the query id
     * @param cols the column names in the partitioned file
     * @param pixelsReader the reader of the partitioned file
     * @param hashValue the hash value of the given hash partition
     * @param numPartition the total number of partitions
     * @return
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

    private PixelsReader getReader(String fileName) throws IOException
    {
        PixelsReaderImpl.Builder builder = PixelsReaderImpl.newBuilder()
                .setStorage(s3)
                .setPath(fileName)
                .setEnableCache(false)
                .setCacheOrder(new ArrayList<>())
                .setPixelsCacheReader(null)
                .setPixelsFooterCache(footerCache);
        PixelsReader pixelsReader = builder.build();
        return pixelsReader;
    }

    private PixelsWriter getWriter(TypeDescription schema, String filePath, boolean encoding)
    {
        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setStorage(minio)
                .setPath(filePath)
                .setOverwrite(true) // set overwrite to true to avoid existence checking.
                .setEncoding(encoding)
                .build();
        return pixelsWriter;
    }
}
