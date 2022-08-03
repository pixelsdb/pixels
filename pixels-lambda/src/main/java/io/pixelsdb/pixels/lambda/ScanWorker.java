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
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.aggregation.Aggregator;
import io.pixelsdb.pixels.executor.lambda.domain.InputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.InputSplit;
import io.pixelsdb.pixels.executor.lambda.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.executor.lambda.domain.StorageInfo;
import io.pixelsdb.pixels.executor.lambda.input.ScanInput;
import io.pixelsdb.pixels.executor.lambda.output.ScanOutput;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.executor.scan.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;
import static java.util.Objects.requireNonNull;

/**
 * The response is a list of files read and then written to s3.
 *
 * @author tiannan
 * @author hank
 * Created in: 03.2022
 */
public class ScanWorker implements RequestHandler<ScanInput, ScanOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(ScanWorker.class);
    private final MetricsCollector metricsCollector = new MetricsCollector();

    @Override
    public ScanOutput handleRequest(ScanInput event, Context context)
    {
        ScanOutput scanOutput = new ScanOutput();
        long startTime = System.currentTimeMillis();
        scanOutput.setStartTimeMs(startTime);
        scanOutput.setRequestId(context.getAwsRequestId());
        scanOutput.setSuccessful(true);
        scanOutput.setErrorMessage("");
        metricsCollector.clear();

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            String requestId = context.getAwsRequestId();

            long queryId = event.getQueryId();
            requireNonNull(event.getTableInfo(), "even.tableInfo is null");
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            boolean[] scanProjection = requireNonNull(event.getScanProjection(),
                    "event.scanProjection is null");
            boolean partialAggregationPresent = event.isPartialAggregationPresent();
            checkArgument(partialAggregationPresent != event.getOutput().isRandomFileName(),
                    "partial aggregation and random output file name should not equal");
            String outputFolder = event.getOutput().getPath();
            StorageInfo storageInfo = event.getOutput().getStorageInfo();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = event.getOutput().isEncoding();
            try
            {
                if (minio == null && storageInfo.getScheme() == Storage.Scheme.minio)
                {
                    ConfigMinIO(event.getOutput().getStorageInfo().getEndpoint(),
                            event.getOutput().getStorageInfo().getAccessKey(),
                            event.getOutput().getStorageInfo().getSecretKey());
                    minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                }
            } catch (Exception e)
            {
                throw new PixelsWorkerException("failed to initialize MinIO storage", e);
            }
            String[] includeCols = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);

            Aggregator aggregator;
            if (partialAggregationPresent)
            {
                logger.info("start get output schema");
                TypeDescription inputSchema = getFileSchema(s3,
                        inputSplits.get(0).getInputInfos().get(0).getPath(), false);
                inputSchema = getResultSchema(inputSchema, includeCols);
                PartialAggregationInfo partialAggregationInfo = event.getPartialAggregationInfo();
                requireNonNull(partialAggregationInfo, "event.partialAggregationInfo is null");
                boolean[] groupKeyProjection = new boolean[partialAggregationInfo.getGroupKeyColumnAlias().length];
                Arrays.fill(groupKeyProjection, true);
                aggregator = new Aggregator(rowBatchSize, inputSchema,
                        partialAggregationInfo.getGroupKeyColumnAlias(),
                        partialAggregationInfo.getGroupKeyColumnIds(), groupKeyProjection,
                        partialAggregationInfo.getAggregateColumnIds(),
                        partialAggregationInfo.getResultColumnAlias(),
                        partialAggregationInfo.getResultColumnTypes(),
                        partialAggregationInfo.getFunctionTypes());
            }
            else
            {
                aggregator = null;
            }

            int outputId = 0;
            logger.info("start scan and aggregate");
            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();
                String outputPath = outputFolder + requestId + "_scan_" + outputId++;

                threadPool.execute(() -> {
                    try
                    {
                        int rowGroupNum = scanFile(queryId, scanInputs, includeCols, scanProjection, filter,
                                outputPath, encoding, storageInfo.getScheme(), partialAggregationPresent, aggregator);
                        if (rowGroupNum > 0)
                        {
                            scanOutput.addOutput(outputPath, rowGroupNum);
                        }
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
                throw new PixelsWorkerException("interrupted while waiting for the termination of scan", e);
            }

            logger.info("start write aggregation result");
            if (partialAggregationPresent)
            {
                String outputPath = event.getOutput().getPath();
                MetricsCollector.Timer writeCostTimer = new MetricsCollector.Timer().start();
                PixelsWriter pixelsWriter = getWriter(aggregator.getOutputSchema(),
                        storageInfo.getScheme() == Storage.Scheme.minio ? minio : s3,
                        outputPath, encoding, false, null);
                aggregator.writeAggrOutput(pixelsWriter);
                pixelsWriter.close();
                if (storageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!minio.exists(outputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }
                metricsCollector.addOutputCostNs(writeCostTimer.stop());
                scanOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
            }

            scanOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            setPerfMetrics(scanOutput, metricsCollector);
            return scanOutput;
        } catch (Exception e)
        {
            logger.error("error during scan", e);
            scanOutput.setSuccessful(false);
            scanOutput.setErrorMessage(e.getMessage());
            scanOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return scanOutput;
        }
    }

    /**
     * Scan the files in a query split, apply projection and filters, and output the
     * results to the given path.
     * @param queryId the query id used by I/O scheduler
     * @param scanInputs the information of the files to scan
     * @param columnsToRead the included columns
     * @param scanProjection whether the column in columnsToRead is included in the scan output
     * @param filter the filter for the scan
     * @param outputPath fileName for the scan results
     * @param encoding whether encode the scan results or not
     * @param outputScheme the storage scheme for the scan result
     * @param partialAggregate whether perform partial aggregation on the scan result
     * @param aggregator the aggregator for the partial aggregation
     * @return the number of row groups that have been written into the output.
     */
    private int scanFile(long queryId, List<InputInfo> scanInputs, String[] columnsToRead,
                         boolean[] scanProjection, TableScanFilter filter, String outputPath, boolean encoding,
                         Storage.Scheme outputScheme, boolean partialAggregate, Aggregator aggregator)
    {
        PixelsWriter pixelsWriter = null;
        Scanner scanner = null;
        if (partialAggregate)
        {
            requireNonNull(aggregator, "aggregator is null whereas partialAggregate is true");
        }
        MetricsCollector.Timer readCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer writeCostTimer = new MetricsCollector.Timer();
        MetricsCollector.Timer computeTimer = new MetricsCollector.Timer();
        for (InputInfo inputInfo : scanInputs)
        {
            readCostTimer.start();
            try (PixelsReader pixelsReader = getReader(inputInfo.getPath(), s3))
            {
                if (inputInfo.getRgStart() >= pixelsReader.getRowGroupNum())
                {
                    continue;
                }
                if (inputInfo.getRgStart() + inputInfo.getRgLength() >= pixelsReader.getRowGroupNum())
                {
                    inputInfo.setRgLength(pixelsReader.getRowGroupNum() - inputInfo.getRgStart());
                }

                PixelsReaderOption option = getReaderOption(queryId, columnsToRead, inputInfo);
                PixelsRecordReader recordReader = pixelsReader.read(option);
                TypeDescription rowBatchSchema = recordReader.getResultSchema();
                VectorizedRowBatch rowBatch;
                readCostTimer.stop();
                metricsCollector.addReadBytes(recordReader.getCompletedBytes());
                metricsCollector.addNumReadRequests(recordReader.getNumReadRequests());

                if (scanner == null)
                {
                    scanner = new Scanner(rowBatchSize, rowBatchSchema, columnsToRead, scanProjection, filter);
                }
                if (pixelsWriter == null && !partialAggregate)
                {
                    writeCostTimer.start();
                    pixelsWriter = getWriter(scanner.getOutputSchema(),
                            outputScheme == Storage.Scheme.minio ? minio : s3,
                            outputPath, encoding, false, null);
                    writeCostTimer.stop();
                }

                computeTimer.start();
                writeCostTimer.start();
                do
                {
                    rowBatch = scanner.filterAndProject(recordReader.readBatch(rowBatchSize));
                    if (rowBatch.size > 0)
                    {
                        if (partialAggregate)
                        {
                            aggregator.aggregate(rowBatch);
                        } else
                        {
                            pixelsWriter.addRowBatch(rowBatch);
                        }
                    }
                } while (!rowBatch.endOfFile);
                writeCostTimer.stop();
                computeTimer.stop();
            } catch (Exception e)
            {
                throw new PixelsWorkerException("failed to scan the file '" +
                        inputInfo.getPath() + "' and output the result", e);
            }
        }
        // Finished scanning all the files in the split.
        try
        {
            int numRowGroup = 0;
            if (pixelsWriter != null)
            {
                writeCostTimer.start();
                pixelsWriter.close();
                if (outputScheme == Storage.Scheme.minio)
                {
                    while (!minio.exists(outputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }
                writeCostTimer.stop();
                metricsCollector.addWriteBytes(pixelsWriter.getCompletedBytes());
                metricsCollector.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                metricsCollector.addOutputCostNs(writeCostTimer.getDuration());
                numRowGroup = pixelsWriter.getNumRowGroup();
            }
            else
            {
                metricsCollector.addComputeCostNs(computeTimer.getDuration());
            }
            metricsCollector.addInputCostNs(readCostTimer.getDuration());


            return numRowGroup;
        } catch (Exception e)
        {
            throw new PixelsWorkerException(
                    "failed finish writing and close the output file '" + outputPath + "'", e);
        }
    }
}
