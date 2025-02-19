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
import io.pixelsdb.pixels.executor.join.Partitioner;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.executor.scan.Scanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2022-05-07
 * @update 2023-04-23 (moved from pixels-worker-lambda to here as the base worker implementation)
 */
public class BasePartitionWorker extends Worker<PartitionInput, PartitionOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BasePartitionWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
    }

    @Override
    public PartitionOutput process(PartitionInput event)
    {
        PartitionOutput partitionOutput = new PartitionOutput();
        long startTime = System.currentTimeMillis();
        partitionOutput.setStartTimeMs(startTime);
        partitionOutput.setRequestId(context.getRequestId());
        partitionOutput.setSuccessful(true);
        partitionOutput.setErrorMessage("");
        workerMetrics.clear();

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            long timestamp = event.getTimestamp();
            requireNonNull(event.getTableInfo(), "event.tableInfo is null");
            StorageInfo inputStorageInfo = event.getTableInfo().getStorageInfo();
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            requireNonNull(event.getPartitionInfo(), "event.partitionInfo is null");
            int numPartition = event.getPartitionInfo().getNumPartition();
            logger.info("table '" + event.getTableInfo().getTableName() +
                    "', number of partitions (" + numPartition + ")");
            int[] keyColumnIds = event.getPartitionInfo().getKeyColumnIds();
            boolean[] projection = event.getProjection();
            requireNonNull(event.getOutput(), "event.output is null");
            StorageInfo outputStorageInfo = requireNonNull(event.getOutput().getStorageInfo(),
                    "output.storageInfo is null");
            String outputPath = event.getOutput().getPath();
            boolean encoding = event.getOutput().isEncoding();

            WorkerCommon.initStorage(inputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            String[] columnsToRead = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);
            AtomicReference<TypeDescription> writerSchema = new AtomicReference<>();
            // The partitioned data would be kept in memory.
            List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitioned = new ArrayList<>(numPartition);
            for (int i = 0; i < numPartition; ++i)
            {
                partitioned.add(new ConcurrentLinkedQueue<>());
            }
            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();

                threadPool.execute(() -> {
                    try
                    {
                        partitionFile(transId, timestamp, scanInputs, columnsToRead, inputStorageInfo.getScheme(),
                                filter, keyColumnIds, projection, partitioned, writerSchema);
                    }
                    catch (Throwable e)
                    {
                        throw new WorkerException("error during partitioning", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                throw new WorkerException("interrupted while waiting for the termination of partitioning", e);
            }

            if (exceptionHandler.hasException())
            {
                throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
            }

            WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
            if (writerSchema.get() == null)
            {
                TypeDescription fileSchema = WorkerCommon.getFileSchemaFromSplits(
                        WorkerCommon.getStorage(inputStorageInfo.getScheme()), inputSplits);
                TypeDescription resultSchema = WorkerCommon.getResultSchema(fileSchema, columnsToRead);
                writerSchema.set(resultSchema);
            }
            PixelsWriter pixelsWriter = WorkerCommon.getWriter(writerSchema.get(),
                    WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath, encoding,
                    true, Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));
            Set<Integer> hashValues = new HashSet<>(numPartition);

            for (int hash = 0; hash < numPartition; ++hash)
            {
                ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitioned.get(hash);
                if (!batches.isEmpty())
                {
                    for (VectorizedRowBatch batch : batches)
                    {
                        pixelsWriter.addRowBatch(batch, hash);
                    }
                    hashValues.add(hash);
                }
            }
            partitionOutput.addOutput(outputPath);
            partitionOutput.setHashValues(hashValues);

            pixelsWriter.close();
            workerMetrics.addOutputCostNs(writeCostTimer.stop());
            workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
            workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());

            partitionOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(partitionOutput, workerMetrics);
            return partitionOutput;
        }
        catch (Throwable e)
        {
            logger.error("error during partition", e);
            partitionOutput.setSuccessful(false);
            partitionOutput.setErrorMessage(e.getMessage());
            partitionOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return partitionOutput;
        }
    }

    /**
     * Scan and partition the files in a query split.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param timestamp the transaction timestamp
     * @param scanInputs the information of the files to scan
     * @param columnsToRead the columns to be read from the input files
     * @param inputScheme the storage scheme of the input files
     * @param filter the filer for the scan
     * @param keyColumnIds the ids of the partition key columns
     * @param projection the projection for the partition
     * @param partitionResult the partition result
     * @param writerSchema the schema to be used for the partition result writer
     */
    private void partitionFile(long transId, long timestamp, List<InputInfo> scanInputs,
                               String[] columnsToRead, Storage.Scheme inputScheme,
                               TableScanFilter filter, int[] keyColumnIds, boolean[] projection,
                               List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult,
                               AtomicReference<TypeDescription> writerSchema)
    {
        Scanner scanner = null;
        Partitioner partitioner = null;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        for (InputInfo inputInfo : scanInputs)
        {
            readCostTimer.start();
            try (PixelsReader pixelsReader = WorkerCommon.getReader(
                    inputInfo.getPath(), WorkerCommon.getStorage(inputScheme)))
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
                PixelsReaderOption option = WorkerCommon.getReaderOption(transId, timestamp, columnsToRead, inputInfo);
                PixelsRecordReader recordReader = pixelsReader.read(option);
                TypeDescription rowBatchSchema = recordReader.getResultSchema();
                VectorizedRowBatch rowBatch;

                if (scanner == null)
                {
                    scanner = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, projection, filter);
                }
                if (partitioner == null)
                {
                    partitioner = new Partitioner(partitionResult.size(), WorkerCommon.rowBatchSize,
                            scanner.getOutputSchema(), keyColumnIds);
                }
                if (writerSchema.get() == null)
                {
                    writerSchema.weakCompareAndSet(null, scanner.getOutputSchema());
                }

                computeCostTimer.start();
                do
                {
                    rowBatch = scanner.filterAndProject(recordReader.readBatch(WorkerCommon.rowBatchSize));
                    if (rowBatch.size > 0)
                    {
                        Map<Integer, VectorizedRowBatch> result = partitioner.partition(rowBatch);
                        if (!result.isEmpty())
                        {
                            for (Map.Entry<Integer, VectorizedRowBatch> entry : result.entrySet())
                            {
                                partitionResult.get(entry.getKey()).add(entry.getValue());
                            }
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
                        inputInfo.getPath() + "' and output the partitioning result", e);
            }
        }
        if (partitioner != null)
        {
            VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
            for (int hash = 0; hash < tailBatches.length; ++hash)
            {
                if (!tailBatches[hash].isEmpty())
                {
                    partitionResult.get(hash).add(tailBatches[hash]);
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
    }
}
