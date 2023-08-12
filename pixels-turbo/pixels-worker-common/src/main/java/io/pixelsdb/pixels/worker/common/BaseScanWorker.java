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

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.aggregation.Aggregator;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.executor.scan.Scanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Scan a table split.
 *
 * @author tiannan
 * @author hank
 * @create 2022-03
 * @update 2023-04-23 (moved from pixels-worker-lambda to here as the base worker implementation)
 */
public class BaseScanWorker extends Worker<ScanInput, ScanOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseScanWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
    }

    @Override
    public ScanOutput process(ScanInput event)
    {
        ScanOutput scanOutput = new ScanOutput();
        long startTime = System.currentTimeMillis();
        scanOutput.setStartTimeMs(startTime);
        scanOutput.setRequestId(context.getRequestId());
        scanOutput.setSuccessful(true);
        scanOutput.setErrorMessage("");
        workerMetrics.clear();

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            requireNonNull(event.getTableInfo(), "even.tableInfo is null");
            StorageInfo inputStorageInfo = event.getTableInfo().getStorageInfo();
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            boolean[] scanProjection = requireNonNull(event.getScanProjection(),
                    "event.scanProjection is null");
            boolean partialAggregationPresent = event.isPartialAggregationPresent();
            String outputFolder = event.getOutput().getPath();
            StorageInfo outputStorageInfo = event.getOutput().getStorageInfo();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = event.getOutput().isEncoding();

            WorkerCommon.initStorage(inputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            String[] includeCols = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);

            Aggregator aggregator;
            if (partialAggregationPresent)
            {
                logger.info("start get output schema");
                TypeDescription inputSchema = WorkerCommon.getFileSchemaFromSplits(
                        WorkerCommon.getStorage(inputStorageInfo.getScheme()), inputSplits);
                inputSchema = WorkerCommon.getResultSchema(inputSchema, includeCols);
                PartialAggregationInfo partialAggregationInfo = event.getPartialAggregationInfo();
                requireNonNull(partialAggregationInfo, "event.partialAggregationInfo is null");
                boolean[] groupKeyProjection = new boolean[partialAggregationInfo.getGroupKeyColumnAlias().length];
                Arrays.fill(groupKeyProjection, true);
                aggregator = new Aggregator(WorkerCommon.rowBatchSize, inputSchema,
                        partialAggregationInfo.getGroupKeyColumnAlias(),
                        partialAggregationInfo.getGroupKeyColumnIds(), groupKeyProjection,
                        partialAggregationInfo.getAggregateColumnIds(),
                        partialAggregationInfo.getResultColumnAlias(),
                        partialAggregationInfo.getResultColumnTypes(),
                        partialAggregationInfo.getFunctionTypes(),
                        partialAggregationInfo.isPartition(),
                        partialAggregationInfo.getNumPartition());
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
                /*
                 * Issue #435:
                 * For table scan without partial aggregation, the path in output info is a folder.
                 * Each scan input split generates an output file in this folder.
                 */
                String outputPath = outputFolder + "scan_" + outputId++;

                threadPool.execute(() -> {
                    try
                    {
                        int rowGroupNum = scanFile(transId, scanInputs, includeCols, inputStorageInfo.getScheme(),
                                scanProjection, filter, outputPath, encoding, outputStorageInfo.getScheme(),
                                partialAggregationPresent, aggregator);
                        if (rowGroupNum > 0)
                        {
                            scanOutput.addOutput(outputPath, rowGroupNum);
                        }
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
                throw new WorkerException("interrupted while waiting for the termination of scan", e);
            }

            if (exceptionHandler.hasException())
            {
                throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
            }

            logger.info("start write aggregation result");
            if (partialAggregationPresent)
            {
                String outputPath = event.getOutput().getPath();
                WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                PixelsWriter pixelsWriter = WorkerCommon.getWriter(aggregator.getOutputSchema(),
                        WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath, encoding,
                        aggregator.isPartition(), aggregator.getGroupKeyColumnIdsInResult());
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
                scanOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
            }

            scanOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(scanOutput, workerMetrics);
            return scanOutput;
        } catch (Throwable e)
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
     * @param transId the transaction id used by I/O scheduler
     * @param scanInputs the information of the files to scan
     * @param columnsToRead the included columns
     * @param inputScheme the storage scheme of the input files
     * @param scanProjection whether the column in columnsToRead is included in the scan output
     * @param filter the filter for the scan
     * @param outputPath fileName for the scan results
     * @param encoding whether encode the scan results or not
     * @param outputScheme the storage scheme for the scan result
     * @param partialAggregate whether perform partial aggregation on the scan result
     * @param aggregator the aggregator for the partial aggregation
     * @return the number of row groups that have been written into the output.
     */
    private int scanFile(long transId, List<InputInfo> scanInputs, String[] columnsToRead, Storage.Scheme inputScheme,
                         boolean[] scanProjection, TableScanFilter filter, String outputPath, boolean encoding,
                         Storage.Scheme outputScheme, boolean partialAggregate, Aggregator aggregator)
    {
        PixelsWriter pixelsWriter = null;
        Scanner scanner = null;
        if (partialAggregate)
        {
            requireNonNull(aggregator, "aggregator is null whereas partialAggregate is true");
        }
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        for (InputInfo inputInfo : scanInputs)
        {
            readCostTimer.start();
            try (PixelsReader pixelsReader = WorkerCommon.getReader(inputInfo.getPath(), WorkerCommon.getStorage(inputScheme)))
            {
                readCostTimer.stop();
                if (inputInfo.getRgStart() >= pixelsReader.getRowGroupNum())
                {
                    continue;
                }
                if (inputInfo.getRgStart() + inputInfo.getRgLength() >= pixelsReader.getRowGroupNum())
                {
                    inputInfo.setRgLength(pixelsReader.getRowGroupNum() - inputInfo.getRgStart());
                }
                PixelsReaderOption option = WorkerCommon.getReaderOption(transId, columnsToRead, inputInfo);
                PixelsRecordReader recordReader = pixelsReader.read(option);
                TypeDescription rowBatchSchema = recordReader.getResultSchema();
                VectorizedRowBatch rowBatch;

                if (scanner == null)
                {
                    scanner = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, scanProjection, filter);
                }
                if (pixelsWriter == null && !partialAggregate)
                {
                    writeCostTimer.start();
                    pixelsWriter = WorkerCommon.getWriter(scanner.getOutputSchema(), WorkerCommon.getStorage(outputScheme),
                            outputPath, encoding, false, null);
                    writeCostTimer.stop();
                }

                computeCostTimer.start();
                do
                {
                    rowBatch = scanner.filterAndProject(recordReader.readBatch(WorkerCommon.rowBatchSize));
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
                computeCostTimer.stop();
                computeCostTimer.minus(recordReader.getReadTimeNanos());
                readCostTimer.add(recordReader.getReadTimeNanos());
                readBytes += recordReader.getCompletedBytes();
                numReadRequests += recordReader.getNumReadRequests();
            } catch (Throwable e)
            {
                throw new WorkerException("failed to scan the file '" +
                        inputInfo.getPath() + "' and output the result", e);
            }
        }
        // Finished scanning all the files in the split.
        try
        {
            int numRowGroup = 0;
            if (pixelsWriter != null)
            {
                // This is a pure scan without aggregation, compute time is the file writing time.
                writeCostTimer.add(computeCostTimer.getElapsedNs());
                writeCostTimer.start();
                pixelsWriter.close();
                if (outputScheme == Storage.Scheme.minio)
                {
                    while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }
                writeCostTimer.stop();
                workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                workerMetrics.addOutputCostNs(writeCostTimer.getElapsedNs());
                numRowGroup = pixelsWriter.getNumRowGroup();
            }
            else
            {
                workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
            }
            workerMetrics.addReadBytes(readBytes);
            workerMetrics.addNumReadRequests(numReadRequests);
            workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
            return numRowGroup;
        } catch (Throwable e)
        {
            throw new WorkerException(
                    "failed finish writing and close the output file '" + outputPath + "'", e);
        }
    }
}
