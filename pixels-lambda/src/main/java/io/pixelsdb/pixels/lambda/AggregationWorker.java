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
import com.google.common.collect.ObjectArrays;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.aggregation.Aggregator;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.lambda.domain.OutputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.StorageInfo;
import io.pixelsdb.pixels.executor.lambda.input.AggregationInput;
import io.pixelsdb.pixels.executor.lambda.output.AggregationOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 08/07/2022
 */
public class AggregationWorker implements RequestHandler<AggregationInput, AggregationOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(AggregationWorker.class);

    @Override
    public AggregationOutput handleRequest(AggregationInput event, Context context)
    {
        AggregationOutput aggregationOutput = new AggregationOutput();
        long startTime = System.currentTimeMillis();
        aggregationOutput.setStartTimeMs(startTime);
        aggregationOutput.setRequestId(context.getAwsRequestId());
        aggregationOutput.setSuccessful(true);
        aggregationOutput.setErrorMessage("");

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);

            long queryId = event.getQueryId();
            List<String> inputFiles = requireNonNull(event.getInputFiles(), "event.inputFiles is null");
            StorageInfo inputStorage = requireNonNull(event.getInputStorage(), "event.inputStorage is null");
            checkArgument(inputStorage.getScheme() == Storage.Scheme.s3,
                    "input storage must be s3");

            FunctionType[] functionTypes = requireNonNull(event.getFunctionTypes(),
                    "event.functionTypes is null");
            String[] groupKeyColumnNames = requireNonNull(event.getGroupKeyColumnNames(),
                    "event.groupKeyColumnIds is null");
            String[] resultColumnNames = requireNonNull(event.getResultColumnNames(),
                    "event.resultColumnNames is null");
            String[] resultColumnTypes = requireNonNull(event.getResultColumnTypes(),
                    "event.resultColumnTypes is null");
            boolean[] groupKeyColumnProj = requireNonNull(event.getGroupKeyColumnProjection(),
                    "event.groupKeyColumnProjection is null");
            checkArgument(groupKeyColumnProj.length == groupKeyColumnNames.length,
                    "group key column names and group key column projection are not of the same length");
            checkArgument(resultColumnNames.length == resultColumnTypes.length,
                    "result column names and result column types are not of the same length");
            int parallelism = event.getParallelism();

            OutputInfo outputInfo = requireNonNull(event.getOutput(), "event.output is null");
            String outputPath = outputInfo.getPath();
            checkArgument(!outputInfo.isRandomFileName(), "output should not be random file");
            StorageInfo outputStorage = requireNonNull(outputInfo.getStorageInfo(),
                    "event.output.storageInfo is null");
            boolean encoding = outputInfo.isEncoding();
            try
            {
                if (minio == null && outputStorage.getScheme() == Storage.Scheme.minio)
                {
                    ConfigMinIO(outputStorage.getEndpoint(), outputStorage.getAccessKey(),
                            outputStorage.getSecretKey());
                    minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                }
            } catch (Exception e)
            {
                throw new PixelsWorkerException("failed to initialize MinIO storage", e);
            }

            String[] includeCols = ObjectArrays.concat(groupKeyColumnNames, resultColumnNames, String.class);
            logger.info("start get output schema");
            TypeDescription inputSchema = getFileSchema(s3, inputFiles.get(0), true);
            checkArgument(inputSchema.getChildren().size() == includeCols.length,
                    "input file does not contain the correct number of columns");
            int[] groupKeyColumnIds = new int[groupKeyColumnNames.length];
            for (int i = 0; i < groupKeyColumnIds.length; ++i)
            {
                groupKeyColumnIds[i] = i;
            }
            int[] aggrColumnIds = new int[resultColumnNames.length];
            for (int i = 0; i < aggrColumnIds.length; ++i)
            {
                aggrColumnIds[i] = i;
            }
            Aggregator aggregator = new Aggregator(rowBatchSize, inputSchema,
                    groupKeyColumnNames, groupKeyColumnIds, groupKeyColumnProj,
                    aggrColumnIds, resultColumnNames, resultColumnTypes, functionTypes);
            logger.info("start scan and aggregate");
            for (int i = 0; i <  inputFiles.size(); ++i)
            {
                List<String> files = new LinkedList<>();
                for (int j = 0; j < parallelism && i < inputFiles.size(); ++j, ++i)
                {
                    files.add(inputFiles.get(i));
                }

                threadPool.execute(() -> {
                    try
                    {
                        aggregate(queryId, files, includeCols, aggregator);
                    }
                    catch (Exception e)
                    {
                        throw new PixelsWorkerException("error during scan", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                throw new PixelsWorkerException("interrupted while waiting for the termination of aggregation", e);
            }

            logger.info("start write aggregation result");

            PixelsWriter pixelsWriter = getWriter(aggregator.getOutputSchema(),
                    outputStorage.getScheme() == Storage.Scheme.minio ? minio : s3,
                    outputPath, encoding, false, null);
            aggregator.writeAggrOutput(pixelsWriter);
            pixelsWriter.close();
            if (outputStorage.getScheme() == Storage.Scheme.minio)
            {
                while (!minio.exists(outputPath))
                {
                    // Wait for 10ms and see if the output file is visible.
                    TimeUnit.MILLISECONDS.sleep(10);
                }
            }
            aggregationOutput.addOutput(outputPath, pixelsWriter.getRowGroupNum());
            aggregationOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));

            return aggregationOutput;
        } catch (Exception e)
        {
            logger.error("error during aggregation", e);
            aggregationOutput.setSuccessful(false);
            aggregationOutput.setErrorMessage(e.getMessage());
            aggregationOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return aggregationOutput;
        }
    }

    /**
     * Scan the files in a query split, apply projection and filters, and output the
     * results to the given path.
     * @param queryId the query id used by I/O scheduler
     * @param inputFiles the paths of the files to read and aggregate
     * @param columnsToRead the columns to read from the input files
     * @param aggregator the aggregator for the partial aggregation
     * @return the number of rows that are read from input files.
     */
    private int aggregate(long queryId, List<String> inputFiles, String[] columnsToRead, Aggregator aggregator)
    {
        requireNonNull(aggregator, "aggregator is null whereas partialAggregate is true");
        int numRows = 0;
        while (!inputFiles.isEmpty())
        {
            for (Iterator<String> it = inputFiles.iterator(); it.hasNext(); )
            {
                String inputFile = it.next();
                try
                {
                    if (s3.exists(inputFile))
                    {
                        it.remove();
                    } else
                    {
                        continue;
                    }
                } catch (IOException e)
                {
                    throw new PixelsWorkerException(
                            "failed to check the existence of the input partial aggregation file '" +
                            inputFile + "'", e);
                }

                try (PixelsReader pixelsReader = getReader(inputFile, s3))
                {
                    PixelsReaderOption option = new PixelsReaderOption();
                    option.queryId(queryId);
                    option.includeCols(columnsToRead);
                    option.rgRange(0, -1);
                    option.skipCorruptRecords(true);
                    option.tolerantSchemaEvolution(true);
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    VectorizedRowBatch rowBatch;

                    do
                    {
                        rowBatch = recordReader.readBatch(rowBatchSize);
                        if (rowBatch.size > 0)
                        {
                            numRows += rowBatch.size;
                            aggregator.aggregate(rowBatch);
                        }
                    } while (!rowBatch.endOfFile);
                } catch (Exception e)
                {
                    throw new PixelsWorkerException("failed to read the input partial aggregation file '" +
                            inputFile + "' and perform aggregation", e);
                }
            }
        }
        return numRows;
    }
}
