/*
 * Copyright 2024 PixelsDB.
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
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.executor.scan.Scanner;
import io.pixelsdb.pixels.executor.utils.Tuple;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.SortInput;
import io.pixelsdb.pixels.planner.plan.physical.output.SortOutput;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class BaseSortWorker extends Worker<SortInput, SortOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseSortWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
    }

    @Override
    public SortOutput process(SortInput event)
    {
        SortOutput sortOutput = new SortOutput();
        long startTime = System.currentTimeMillis();
        long timestamp = event.getTimestamp();
        sortOutput.setStartTimeMs(startTime);
        sortOutput.setRequestId(context.getRequestId());
        sortOutput.setSuccessful(true);
        sortOutput.setErrorMessage("");
        workerMetrics.clear();

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            requireNonNull(event.getTableInfo(), "event.tableInfo is null");
            StorageInfo inputStorageInfo = event.getTableInfo().getStorageInfo();
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            int[] keyColumnIds = event.getKeyColumnIds();
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
            ConcurrentLinkedQueue<VectorizedRowBatch> result = new ConcurrentLinkedQueue<>();
            List<List<Tuple>> resultToMerge = Collections.synchronizedList(new LinkedList<>());
            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();

                threadPool.execute(() -> {
                    try
                    {
                        List<Tuple> sortList = new LinkedList<>();
                        sortFile(transId, timestamp, scanInputs, columnsToRead, inputStorageInfo.getScheme(),
                                filter, keyColumnIds, projection, sortList, writerSchema);
                        resultToMerge.add(sortList);
                    } catch (Throwable e)
                    {
                        throw new WorkerException("error during partitioning", e);
                    }
                });
            }
            threadPool.shutdown();

            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) ;
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
            mergeSortedList(resultToMerge, result, writerSchema);
            PixelsWriter pixelsWriter = WorkerCommon.getWriter(writerSchema.get(),
                    WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath, encoding,
                    false, Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));

            for (VectorizedRowBatch batch : result)
            {
                pixelsWriter.addRowBatch(batch);
            }

            sortOutput.addOutput(outputPath);
            pixelsWriter.close();
            workerMetrics.addOutputCostNs(writeCostTimer.stop());
            workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
            workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());

            sortOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(sortOutput, workerMetrics);
            return sortOutput;
        } catch (Throwable e)
        {
            logger.error("error during partition", e);
            sortOutput.setSuccessful(false);
            sortOutput.setErrorMessage(e.getMessage());
            sortOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return sortOutput;
        }
    }

    /**
     * Scan and sort the files in a query split.
     *
     * @param transId       the transaction id used by I/O scheduler
     * @param scanInputs    the information of the files to scan
     * @param columnsToRead the columns to be read from the input files
     * @param inputScheme   the storage scheme of the input files
     * @param filter        the filer for the scan
     * @param keyColumnIds  the ids of the sort key columns
     * @param projection    the projection for the partition
     * @param result        the sort result
     * @param writerSchema  the schema to be used for the sort result writer
     */
    private void sortFile(long transId, long timestamp, List<InputInfo> scanInputs,
                          String[] columnsToRead, Storage.Scheme inputScheme,
                          TableScanFilter filter, int[] keyColumnIds, boolean[] projection,
                          List<Tuple> result,
                          AtomicReference<TypeDescription> writerSchema)
    {
        io.pixelsdb.pixels.executor.scan.Scanner scanner = null;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;

        VectorizedRowBatch rowBatch;
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

                if (scanner == null)
                {
                    scanner = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, projection, filter);
                }
                if (writerSchema.get() == null)
                {
                    writerSchema.weakCompareAndSet(null, scanner.getOutputSchema());
                }
                rowBatch = writerSchema.get().createRowBatch(); // rowbatch size

                computeCostTimer.start();
                do
                {
                    rowBatch = scanner.filterAndProject(recordReader.readBatch(WorkerCommon.rowBatchSize));
                    if (rowBatch.size > 0)
                    {
                        Tuple.Builder builder = new Tuple.Builder(rowBatch, keyColumnIds, projection);
                        while (builder.hasNext())
                        {
                            Tuple tuple = builder.next();
                            result.add(tuple);
                        }
                    }

                } while (!rowBatch.endOfFile);
                result.sort(Comparator.naturalOrder());
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
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
    }


    private void mergeSortedList(List<List<Tuple>> sortList, ConcurrentLinkedQueue<VectorizedRowBatch> result, AtomicReference<TypeDescription> writerSchema)
    {
        class Pair
        {
            Tuple tuple;
            int listIndex;
            int tupleIndex;

            public Pair(Tuple tuple, int listIndex, int tupleIndex)
            {
                this.tuple = tuple;
                this.listIndex = listIndex;
                this.tupleIndex = tupleIndex;
            }
        }

        PriorityQueue<Pair> pq = new PriorityQueue<>(Comparator.comparing(p -> p.tuple));
        VectorizedRowBatch rowBatch = writerSchema.get().createRowBatch();
        int index = 0;
        for (List<Tuple> list : sortList)
        {
            if (!list.isEmpty())
            {
                pq.add(new Pair(list.get(0), index, 0));
            }
            index++;
        }

        while (!pq.isEmpty())
        {
            Pair pair = pq.poll();
            Tuple minTuple = pair.tuple;
            if (rowBatch.isFull())
            {
                result.add(rowBatch);
                rowBatch = writerSchema.get().createRowBatch();
            }
            minTuple.writeTo(rowBatch);
            int listIndex = pair.listIndex;
            int nextTupleIndex = pair.tupleIndex + 1;
            if (nextTupleIndex < sortList.get(listIndex).size())
            {
                pq.add(new Pair(sortList.get(listIndex).get(nextTupleIndex), listIndex, nextTupleIndex));
            }
        }

        if (!rowBatch.isEmpty())
        {
            result.add(rowBatch);
        }
    }
}
