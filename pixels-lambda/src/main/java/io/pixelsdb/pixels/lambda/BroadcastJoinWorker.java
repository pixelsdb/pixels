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
import io.pixelsdb.pixels.executor.lambda.BroadcastJoinInput;
import io.pixelsdb.pixels.executor.lambda.JoinOutput;
import io.pixelsdb.pixels.executor.lambda.ScanInput;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;

/**
 * @author hank
 * @date 07/05/2022
 */
public class BroadcastJoinWorker implements RequestHandler<BroadcastJoinInput, JoinOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionedJoinWorker.class);
    private static Storage s3;
    private static Storage minio;

    static
    {
        try
        {
            s3 = StorageFactory.Instance().getStorage(Storage.Scheme.s3);

        } catch (Exception e)
        {
            logger.error("failed to initialize AWS S3 storage", e);
        }
    }

    @Override
    public JoinOutput handleRequest(BroadcastJoinInput event, Context context)
    {
        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            String requestId = context.getAwsRequestId();

            long queryId = event.getQueryId();

            BroadcastJoinInput.TableInfo leftTable = event.getLeftTable();
            List<ScanInput.InputInfo> leftInputs = leftTable.getInputs();
            checkArgument(leftInputs.size() > 0, "leftPartitioned is empty");
            String[] leftCols = leftTable.getCols();
            int[] leftKeyColumnIds = leftTable.getKeyColumnIds();
            int leftSplitSize = leftTable.getSplitSize();
            TableScanFilter leftFilter = JSON.parseObject(leftTable.getFilter(), TableScanFilter.class);

            BroadcastJoinInput.TableInfo rightTable = event.getRightTable();
            List<ScanInput.InputInfo> rightInputs = rightTable.getInputs();
            checkArgument(rightInputs.size() > 0, "rightPartitioned is empty");
            String[] rightCols = rightTable.getCols();
            int[] rightKeyColumnIds = rightTable.getKeyColumnIds();
            int rightSplitSize = rightTable.getSplitSize();
            TableScanFilter rightFilter = JSON.parseObject(rightTable.getFilter(), TableScanFilter.class);

            String[] joinedCols = event.getJoinedCols();
            JoinType joinType = event.getJoinType();
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
            getFileSchema(threadPool, s3, leftSchema, rightSchema,
                    leftInputs.get(0).getPath(), rightInputs.get(0).getPath());
            Joiner joiner = new Joiner(joinType, joinedCols,
                    getResultSchema(leftSchema.get(), leftCols), leftKeyColumnIds,
                    getResultSchema(rightSchema.get(), rightCols), rightKeyColumnIds);
            // build the hash table for the left table.
            List<Future> leftFutures = new ArrayList<>();
            for (int i = 0; i < leftInputs.size();)
            {
                List<ScanInput.InputInfo> inputs = new ArrayList<>();
                int numRg = 0;
                while (numRg < leftSplitSize && i < leftInputs.size())
                {
                    ScanInput.InputInfo info = leftInputs.get(i++);
                    inputs.add(info);
                    numRg += info.getRgLength();
                }
                leftFutures.add(threadPool.submit(() -> {
                    try
                    {
                        buildHashTable(queryId, joiner, inputs, leftCols, leftFilter);
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
            for (int i = 0, outputId = 0; i < rightInputs.size(); ++outputId)
            {
                List<ScanInput.InputInfo> inputs = new ArrayList<>();
                int numRg = 0;
                while (numRg < rightSplitSize && i < rightInputs.size())
                {
                    ScanInput.InputInfo info = rightInputs.get(i++);
                    inputs.add(info);
                    numRg += info.getRgLength();
                }
                String outputPath = outputFolder + requestId + "_join_" + outputId;
                threadPool.execute(() -> {
                    try
                    {
                        int rowGroupNum = joinWithRightTable(queryId, joiner, inputs, rightCols,
                                rightFilter, outputPath, encoding);
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
                String outputPath = outputFolder + requestId + "_join_left_outer";
                PixelsWriter pixelsWriter = getWriter(joiner.getJoinedSchema(), minio, outputPath,
                        encoding, false, null);
                joiner.writeLeftOuter(pixelsWriter, rowBatchSize);
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
     * @param leftCols the column names of the left table
     * @param leftFilter the table scan filter on the left table
     */
    private void buildHashTable(long queryId, Joiner joiner, List<ScanInput.InputInfo> leftInputs,
                                String[] leftCols, TableScanFilter leftFilter)
    {
        while (!leftInputs.isEmpty())
        {
            for (Iterator<ScanInput.InputInfo> it = leftInputs.iterator(); it.hasNext(); )
            {
                ScanInput.InputInfo input = it.next();
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
     * @param rightCols the column names of the right table
     * @param rightFilter the table scan filter on the right table
     * @param outputPath fileName on s3 to store the scan results
     * @param encoding whether encode the scan results or not
     * @return the number of row groups that have been written into the output.
     */
    private int joinWithRightTable(long queryId, Joiner joiner, List<ScanInput.InputInfo> rightInputs,
                                   String[] rightCols, TableScanFilter rightFilter,
                                   String outputPath, boolean encoding)
    {
        PixelsWriter pixelsWriter = getWriter(joiner.getJoinedSchema(), minio, outputPath,
                encoding, false, null);
        while (!rightInputs.isEmpty())
        {
            for (Iterator<ScanInput.InputInfo> it = rightInputs.iterator(); it.hasNext(); )
            {
                ScanInput.InputInfo input = it.next();
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
     * Create the reader option for a record reader of the given input file.
     *
     * @param queryId the query id
     * @param cols the column names in the partitioned file
     * @param input the information of the input file
     * @return the reader option
     */
    private PixelsReaderOption getReaderOption(long queryId, String[] cols, ScanInput.InputInfo input)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.queryId(queryId);
        option.includeCols(cols);
        option.rgRange(input.getRgStart(), input.getRgLength());
        return option;
    }
}
