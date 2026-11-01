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
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionMemoryPolicy;
import io.pixelsdb.pixels.planner.plan.physical.PartitionInputSizing;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import io.pixelsdb.pixels.storage.s3qs.S3QueueMessage;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
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
    private PartitionMemoryAccount memory;

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
        ExecutorService threadPool = null;
        S3QSAsyncPartitionOutput producerOutput = null;
        List<ConcurrentLinkedQueue<VectorizedRowBatch>> retained = null;
        S3QSQueueScope queueScope = new S3QSQueueScope();
        memory = null;
        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            context.getCancellation().check();
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger, context.getCancellation());

            long transId = event.getTransId();
            long timestamp = event.getTimestamp();
            requireNonNull(event.getTableInfo(), "event.tableInfo is null");
            StorageInfo inputStorageInfo = event.getTableInfo().getStorageInfo();
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            PartitionMemoryPolicy policy = event.getMemoryPolicy();
            if (policy == null && PartitionMemoryPolicy.enabled("worker"))
            {
                long bytes = context.getMemoryBytes() > 0 ? context.getMemoryBytes() : Runtime.getRuntime().maxMemory();
                policy = PartitionMemoryPolicy.forResources(bytes, cores);
            }
            // Keep conservative plans; only a smaller actual platform limit tightens them.
            if (policy != null && context.getMemoryBytes() > 0 && policy.getWorkerBytes() > context.getMemoryBytes())
            {
                policy = PartitionMemoryPolicy.forResources(context.getMemoryBytes(), cores);
            }
            if (policy != null)
            {
                if (inputStorageInfo.getScheme() == Storage.Scheme.httpstream)
                {
                    throw new WorkerException("partition memory windows require a seekable file input");
                }
                memory = new PartitionMemoryAccount(policy, inputSplits.size());
            }
            int readers = memory == null ? cores * 2 : memory.policy.getReaderThreads();
            threadPool = context.createThreadPool(Math.max(1, Math.min(readers, inputSplits.size())), exceptionHandler);
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
            queueScope.register(event.getOutput().getShuffleInfo());

            String[] columnsToRead = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);
            ShuffleInfo shuffleInfo = event.getOutput().getShuffleInfo();
            try (S3QSAsyncPartitionOutput asyncOutput = isS3QSShuffle(shuffleInfo) ?
                    createS3QSOutput(event, shuffleInfo, numPartition) : null)
            {
                queueScope.own(asyncOutput);
                producerOutput = asyncOutput;
                if (asyncOutput != null) { asyncOutput.readerLimit(Math.max(1, Math.min(readers, inputSplits.size()))); }
                AtomicReference<TypeDescription> writerSchema = new AtomicReference<>();
                // Batch shuffle retains batches here; S3QS transfers them to bounded background jobs.
                List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitioned = new ArrayList<>(numPartition);
                retained = partitioned;
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
                            if (asyncOutput == null)
                            {
                                partitionFile(transId, timestamp, scanInputs, columnsToRead, inputStorageInfo.getScheme(),
                                        filter, keyColumnIds, projection, partitioned, writerSchema);
                            }
                            else
                            {
                                partitionFile(transId, timestamp, scanInputs, columnsToRead, inputStorageInfo.getScheme(),
                                        filter, keyColumnIds, projection, partitioned, writerSchema, asyncOutput);
                            }
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
                    while (!threadPool.awaitTermination(100, TimeUnit.MILLISECONDS))
                    {
                        context.getCancellation().check();
                    }
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the termination of partitioning", e);
                }

                if (exceptionHandler.hasException())
                {
                    throw new WorkerException("error during partitioning", exceptionHandler.getException());
                }

                context.getCancellation().check();
                WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                if (writerSchema.get() == null)
                {
                    TypeDescription fileSchema = WorkerCommon.getFileSchemaFromSplits(
                            WorkerCommon.getStorage(inputStorageInfo.getScheme()), inputSplits);
                    TypeDescription resultSchema = WorkerCommon.getResultSchema(fileSchema, columnsToRead);
                    writerSchema.set(resultSchema);
                }
                Set<Integer> hashValues = new HashSet<>(numPartition);
                if (asyncOutput != null)
                {
                    asyncOutput.finish(writerSchema.get(), hashValues);
                }
                else
                {
                    PixelsWriter pixelsWriter = WorkerCommon.getWriter(writerSchema.get(),
                            WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath, encoding,
                            true, Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));

                    for (int hash = 0; hash < numPartition; ++hash)
                    {
                        ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitioned.get(hash);
                        if (!batches.isEmpty())
                        {
                            VectorizedRowBatch batch;
                            while ((batch = batches.poll()) != null)
                            {
                                try { pixelsWriter.addRowBatch(batch, hash); }
                                finally { if (memory != null) { memory.output.release(batch.getMemoryUsage()); } }
                            }
                            hashValues.add(hash);
                        }
                    }

                    pixelsWriter.close();
                    workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                    workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                    workerMetrics.addOutputCostNs(writeCostTimer.stop());
                }
                partitionOutput.addOutput(outputPath);
                partitionOutput.setHashValues(hashValues);
            }

            partitionOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(partitionOutput, workerMetrics);
            return partitionOutput;
        }
        catch (Throwable e)
        {
            logger.error("error during partition", e);
            partitionOutput.setSuccessful(false);
            partitionOutput.setErrorMessage(failureMessage(e));
            partitionOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return partitionOutput;
        }
        finally
        {
            queueScope.closeAfter(threadPool);
            if (threadPool != null)
            {
                threadPool.shutdownNow();
                if (memory != null)
                {
                    try { threadPool.awaitTermination(5, TimeUnit.SECONDS); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                }
            }
            if (memory != null)
            {
                if (retained != null && (threadPool == null || threadPool.isTerminated()))
                {
                    for (ConcurrentLinkedQueue<VectorizedRowBatch> queue : retained)
                    {
                        VectorizedRowBatch batch;
                        while ((batch = queue.poll()) != null) { memory.output.release(batch.getMemoryUsage()); }
                    }
                }
                Map<String, Long> metrics = memory.snapshot();
                metrics.put("planningMemoryBytes", memory.policy.getWorkerBytes());
                metrics.put("platformMemoryBytes", context.getMemoryBytes());
                if (producerOutput != null) { metrics.putAll(producerOutput.snapshot()); }
                partitionOutput.setMemoryMetrics(metrics);
                logger.info("Partition memory: " + JSON.toJSONString(metrics));
            }
        }
    }

    static boolean isS3QSShuffle(ShuffleInfo shuffleInfo)
    {
        return shuffleInfo != null &&
                shuffleInfo.getStorageInfo() != null &&
                shuffleInfo.getStorageInfo().getScheme() == Storage.Scheme.s3qs;
    }

    static S3QueueMessage createS3QSDataMessage(ShuffleInfo shuffleInfo, int partitionId,
                                                int producerTaskId, int producerAttemptId,
                                                long sequenceId, TypeDescription writerSchema)
    {
        return S3QueueMessage.data(shuffleInfo.getShuffleId(), partitionId,
                producerTaskId, producerAttemptId, sequenceId,
                shuffleInfo.getObjectPathPrefix())
                .setMetadata(writerSchema.toString());
    }

    static S3QueueMessage createS3QSProducerEndMessage(ShuffleInfo shuffleInfo, int partitionId,
                                                       int producerTaskId, int producerAttemptId,
                                                       long sequenceId, TypeDescription writerSchema)
    {
        return S3QueueMessage.producerEnd(shuffleInfo.getShuffleId(), partitionId,
                producerTaskId, producerAttemptId, sequenceId)
                .setMetadata(writerSchema.toString());
    }

    private S3QSAsyncPartitionOutput createS3QSOutput(PartitionInput event, ShuffleInfo shuffleInfo, int count)
    {
        if (event.getProducerTaskId() < 0)
        {
            throw new WorkerException("producerTaskId is not set for s3qs shuffle");
        }
        Storage storage = WorkerCommon.getStorage(Storage.Scheme.s3qs);
        if (!(storage instanceof S3QS))
        {
            throw new WorkerException("storage of scheme s3qs is not S3QS");
        }
        return new S3QSAsyncPartitionOutput((S3QS) storage, shuffleInfo, event.getProducerTaskId(), count, context, memory == null ? null : memory.output);
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
    protected void partitionFile(long transId, long timestamp, List<InputInfo> scanInputs,
                                 String[] columnsToRead, Storage.Scheme inputScheme,
                                 TableScanFilter filter, int[] keyColumnIds, boolean[] projection,
                                 List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult,
                                 AtomicReference<TypeDescription> writerSchema)
    {
        partitionFile(transId, timestamp, scanInputs, columnsToRead, inputScheme, filter,
                keyColumnIds, projection, partitionResult, writerSchema, null);
    }

    protected void partitionFile(long transId, long timestamp, List<InputInfo> scanInputs,
                                 String[] columnsToRead, Storage.Scheme inputScheme,
                                 TableScanFilter filter, int[] keyColumnIds, boolean[] projection,
                                 List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult,
                                 AtomicReference<TypeDescription> writerSchema, S3QSAsyncPartitionOutput output)
    {
        Scanner scanner = null;
        Partitioner partitioner = null;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        for (InputInfo inputInfo : scanInputs)
        {
            if (output != null) { output.check(); }
            context.getCancellation().check();
            readCostTimer.start();
            try (PixelsReader pixelsReader = WorkerCommon.getReader(
                    inputInfo.getPath(), WorkerCommon.getStorage(inputScheme)))
            {
                readCostTimer.stop();
                if (inputInfo.getRgStart() >= pixelsReader.getRowGroupNum())
                {
                    continue;
                }
                int fileRequests = 0;
                int end = PartitionInputSizing.end(inputInfo, pixelsReader.getRowGroupNum());
                for (int start = inputInfo.getRgStart(); start < end;)
                {
                    if (output != null) { output.acquireScan(); }
                    try
                    {
                        long sizingStart = System.nanoTime();
                        PartitionReadWindow window = memory == null ? null : PartitionReadWindow.next(
                                pixelsReader, inputInfo, start, end, columnsToRead, memory.policy, memory.input.limit());
                        long sizingNs = System.nanoTime() - sizingStart;
                        InputInfo range = window == null ? new InputInfo(inputInfo.getPath(), start, end - start) : window.input;
                        if (memory != null)
                        {
                            memory.input.acquire(window.bytes, context.getCancellation()::check);
                            memory.window(window, sizingNs);
                        }
                        PixelsRecordReader recordReader = null;
                        long scanBegin = System.nanoTime(), transferNs = 0, producedBytes = 0;
                        try
                        {
                            PixelsReaderOption option = WorkerCommon.getReaderOption(transId, timestamp, columnsToRead, range);
                            recordReader = pixelsReader.read(option);
                            TypeDescription rowBatchSchema = recordReader.getResultSchema();
                            if (scanner == null)
                            {
                                scanner = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, projection, filter);
                            }
                            if (partitioner == null)
                            {
                                if (memory != null) { memory.validatePartitioner(scanner.getOutputSchema(), partitionResult.size()); }
                                partitioner = new Partitioner(partitionResult.size(), WorkerCommon.rowBatchSize,
                                        scanner.getOutputSchema(), keyColumnIds, memory != null);
                            }
                            writerSchema.compareAndSet(null, scanner.getOutputSchema());
                            computeCostTimer.start();
                            VectorizedRowBatch rowBatch;
                            do
                            {
                                if (output != null) { output.check(); }
                                context.getCancellation().check();
                                rowBatch = scanner.filterAndProject(recordReader.readBatch(WorkerCommon.rowBatchSize));
                                if (rowBatch.size > 0)
                                {
                                    Map<Integer, VectorizedRowBatch> result = partitioner.partition(rowBatch);
                                    if (memory != null) { memory.checkTailBytes(partitioner.getRetainedMemoryUsage()); }
                                    for (Map.Entry<Integer, VectorizedRowBatch> entry : result.entrySet())
                                    {
                                        long transferBegin = System.nanoTime();
                                        emitPartitionBatch(partitionResult, output, entry.getKey(), entry.getValue(), scanner.getOutputSchema());
                                        transferNs += System.nanoTime() - transferBegin;
                                        producedBytes += entry.getValue().getMemoryUsage();
                                    }
                                }
                            } while (!rowBatch.endOfFile);
                            if (output != null) { output.scanned(producedBytes, System.nanoTime() - scanBegin - transferNs); }
                            computeCostTimer.stop();
                            computeCostTimer.minus(recordReader.getReadTimeNanos());
                            readCostTimer.add(recordReader.getReadTimeNanos());
                            readBytes += recordReader.getCompletedBytes();
                            fileRequests = recordReader.getNumReadRequests();
                        }
                        finally
                        {
                            // Close decoder/chunk references before allowing another window to allocate.
                            try { if (recordReader != null) { pixelsReader.closeRecordReader(recordReader); } }
                            finally { if (memory != null) { memory.input.release(window.bytes); } }
                        }
                        start += range.getRgLength();
                    }
                    finally { if (output != null) { output.releaseScan(); } }
                }
                numReadRequests += fileRequests;
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
                    emitPartitionBatch(partitionResult, output, hash, tailBatches[hash], scanner.getOutputSchema());
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
    }

    private void emitPartitionBatch(List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitions,
                                          S3QSAsyncPartitionOutput output, int partition,
                                          VectorizedRowBatch batch, TypeDescription schema)
    {
        if (output == null)
        {
            if (memory != null && !memory.output.tryAcquire(batch.getMemoryUsage()))
            {
                throw new WorkerException("Batch retained output exceeds memory budget; enable planner memory limits or reduce task size");
            }
            partitions.get(partition).add(batch);
        }
        // Partitioner replaces full batches; its tail is handed off only after scanning ends.
        else { output.add(partition, batch, schema); }
    }
}
