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

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.aggregation.Aggregator;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregatedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2022-07-08
 * @update 2023-04-23 (moved from pixels-worker-lambda to here as the base worker implementation)
 */
public class BaseAggregationWorker extends Worker<AggregationInput, AggregationOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseAggregationWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    @Override
    public AggregationOutput process(AggregationInput event)
    {
        AggregationOutput aggregationOutput = new AggregationOutput();
        long startTime = System.currentTimeMillis();
        aggregationOutput.setStartTimeMs(startTime);
        aggregationOutput.setRequestId(context.getRequestId());
        aggregationOutput.setSuccessful(true);
        aggregationOutput.setErrorMessage("");

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            AggregationInfo aggregationInfo = requireNonNull(event.getAggregationInfo(),
                    "event.aggregationInfo is null");
            boolean inputPartitioned = aggregationInfo.isInputPartitioned();
            List<Integer> hashValues;
            int numPartition = aggregationInfo.getNumPartition();
            if (inputPartitioned)
            {
                hashValues = requireNonNull(aggregationInfo.getHashValues(),
                        "aggregationInfo.hashValues is null");
                checkArgument(!hashValues.isEmpty(), "aggregationInfo.hashValues is empty");
            }
            else
            {
                hashValues = ImmutableList.of();
            }
            AggregatedTableInfo aggregatedTableInfo = requireNonNull(event.getAggregatedTableInfo(),
                    "event.aggregatedTableInfo is null");
            List<String> inputFiles = requireNonNull(aggregatedTableInfo.getInputFiles(),
                    "aggregatedTableInfo.inputFiles is null");
            StorageInfo inputStorageInfo = requireNonNull(aggregatedTableInfo.getStorageInfo(),
                    "aggregatedTableInfo.storageInfo is null");

            FunctionType[] functionTypes = requireNonNull(aggregationInfo.getFunctionTypes(),
                    "aggregationInfo.functionTypes is null");
            String[] columnsToRead = requireNonNull(aggregatedTableInfo.getColumnsToRead(),
                    "aggregatedTableInfo.columnsToRead is null");
            int[] groupKeyColumnIds = requireNonNull(aggregationInfo.getGroupKeyColumnIds(),
                    "aggregationInfo.groupKeyColumnIds is null");
            int[] aggrColumnIds = requireNonNull(aggregationInfo.getAggregateColumnIds(),
                    "aggregationInfo.aggregateColumnIds is null");
            String[] groupKeyColumnNames = requireNonNull(aggregationInfo.getGroupKeyColumnNames(),
                    "aggregationInfo.groupKeyColumnIds is null");
            String[] resultColumnNames = requireNonNull(aggregationInfo.getResultColumnNames(),
                    "aggregationInfo.resultColumnNames is null");
            String[] resultColumnTypes = requireNonNull(aggregationInfo.getResultColumnTypes(),
                    "aggregationInfo.resultColumnTypes is null");
            boolean[] groupKeyColumnProj = requireNonNull(aggregationInfo.getGroupKeyColumnProjection(),
                    "aggregationInfo.groupKeyColumnProjection is null");
            checkArgument(groupKeyColumnProj.length == groupKeyColumnNames.length,
                    "group key column names and group key column projection are not of the same length");
            checkArgument(resultColumnNames.length == resultColumnTypes.length,
                    "result column names and result column types are not of the same length");
            int parallelism = aggregatedTableInfo.getParallelism();

            OutputInfo outputInfo = requireNonNull(event.getOutput(), "event.output is null");
            String outputPath = outputInfo.getPath();
            StorageInfo outputStorageInfo = requireNonNull(outputInfo.getStorageInfo(),
                    "event.output.storageInfo is null");
            boolean encoding = outputInfo.isEncoding();

            WorkerCommon.initStorage(inputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            TypeDescription fileSchema = WorkerCommon.getFileSchemaFromPaths(
                    WorkerCommon.getStorage(inputStorageInfo.getScheme()), inputFiles);
            /*
             * Issue #450:
             * For the partial aggregate files, the file schema is equal to the columns to read in normal cases.
             * However, it is safer to turn file schema into result schema here.
             */
            checkArgument(fileSchema.getChildren().size() == columnsToRead.length,
                    "input file does not contain the correct number of columns");
            TypeDescription inputSchema = WorkerCommon.getResultSchema(fileSchema, columnsToRead);

            // start aggregation.
            for (int i = 0; i < functionTypes.length; ++i)
            {
                if (functionTypes[i] == FunctionType.COUNT)
                {
                    functionTypes[i] = FunctionType.SUM;
                }
            }
            Aggregator aggregator = new Aggregator(WorkerCommon.rowBatchSize, inputSchema, groupKeyColumnNames,
                    groupKeyColumnIds, groupKeyColumnProj, aggrColumnIds, resultColumnNames,
                    resultColumnTypes, functionTypes, false, 0);
            for (int i = 0; i <  inputFiles.size(); )
            {
                List<String> files = new LinkedList<>();
                for (int j = 0; j < parallelism && i < inputFiles.size(); ++j, ++i)
                {
                    files.add(inputFiles.get(i));
                }

                threadPool.execute(() -> {
                    try
                    {
                        aggregate(transId, files, columnsToRead, inputStorageInfo.getScheme(),
                                hashValues, numPartition, aggregator, workerMetrics);
                    }
                    catch (Throwable e)
                    {
                        throw new WorkerException("error during scan", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                throw new WorkerException("interrupted while waiting for the termination of aggregation", e);
            }

            if (exceptionHandler.hasException())
            {
                throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
            }

            WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
            PixelsWriter pixelsWriter = WorkerCommon.getWriter(aggregator.getOutputSchema(),
                    WorkerCommon.getStorage(outputStorageInfo.getScheme()),
                    outputPath, encoding, false, null);
            aggregator.writeAggrOutput(pixelsWriter);
            pixelsWriter.close();
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
            aggregationOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
            aggregationOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(aggregationOutput, workerMetrics);
            return aggregationOutput;
        } catch (Throwable e)
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
     * @param transId the transaction id used by I/O scheduler
     * @param inputFiles the paths of the files to read and aggregate
     * @param columnsToRead the columns to read from the input files
     * @param inputScheme the storage scheme of the input files
     * @param hashValues the hashValues of the partitions to be read from the input files,
     *                   empty if input files are not partitioned
     * @param numPartition the number of partitions for the input files
     * @param aggregator the aggregator for the partial aggregation
     * @param workerMetrics the collector of the performance metrics
     * @return the number of rows that are read from input files
     */
    private int aggregate(long transId, List<String> inputFiles, String[] columnsToRead,
                          Storage.Scheme inputScheme, List<Integer> hashValues,
                          int numPartition, Aggregator aggregator, WorkerMetrics workerMetrics)
    {
        requireNonNull(aggregator, "aggregator is null whereas partialAggregate is true");
        int numRows = 0;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!inputFiles.isEmpty())
        {
            for (Iterator<String> it = inputFiles.iterator(); it.hasNext(); )
            {
                String inputFile = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        inputFile, WorkerCommon.getStorage(inputScheme)))
                {
                    readCostTimer.stop();
                    if (pixelsReader.getRowGroupNum() == 0)
                    {
                        it.remove();
                        continue;
                    }
                    if (hashValues.isEmpty())
                    {
                        PixelsReaderOption option = new PixelsReaderOption();
                        option.transId(transId);
                        option.includeCols(columnsToRead);
                        option.rgRange(0, -1);
                        option.skipCorruptRecords(true);
                        option.tolerantSchemaEvolution(true);
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        VectorizedRowBatch rowBatch;

                        computeCostTimer.start();
                        do
                        {
                            rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                            if (rowBatch.size > 0)
                            {
                                numRows += rowBatch.size;
                                aggregator.aggregate(rowBatch);
                            }
                        } while (!rowBatch.endOfFile);
                        computeCostTimer.stop();
                        computeCostTimer.minus(recordReader.getReadTimeNanos());
                        readCostTimer.add(recordReader.getReadTimeNanos());
                        readBytes += recordReader.getCompletedBytes();
                        numReadRequests += recordReader.getNumReadRequests();
                    }
                    else
                    {
                        checkArgument(pixelsReader.isPartitioned(), "input file is not partitioned");
                        Set<Integer> existHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                        for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                        {
                            existHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                        }
                        for (int hashValue : hashValues)
                        {
                            if (!existHashValues.contains(hashValue))
                            {
                                continue;
                            }
                            PixelsReaderOption option = WorkerCommon.getReaderOption(transId, columnsToRead, pixelsReader,
                                    hashValue, numPartition);
                            PixelsRecordReader recordReader = pixelsReader.read(option);
                            VectorizedRowBatch rowBatch;

                            computeCostTimer.start();
                            do
                            {
                                rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                                if (rowBatch.size > 0)
                                {
                                    numRows += rowBatch.size;
                                    aggregator.aggregate(rowBatch);
                                }
                            } while (!rowBatch.endOfFile);
                            computeCostTimer.stop();
                            computeCostTimer.minus(recordReader.getReadTimeNanos());
                            readCostTimer.add(recordReader.getReadTimeNanos());
                            readBytes += recordReader.getCompletedBytes();
                            numReadRequests += recordReader.getNumReadRequests();
                        }
                    }
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to read the input partial aggregation file '" +
                            inputFile + "' and perform aggregation", e);
                }
            }
            if (!inputFiles.isEmpty())
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
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        return numRows;
    }
}
