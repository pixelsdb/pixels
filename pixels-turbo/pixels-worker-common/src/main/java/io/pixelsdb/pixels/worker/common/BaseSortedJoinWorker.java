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

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.SortedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BaseSortedJoinWorker extends Worker<SortedJoinInput, JoinOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseSortedJoinWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    @Override
    public JoinOutput process(SortedJoinInput event)
    {
        JoinOutput joinOutput = new JoinOutput();
        long startTime = System.currentTimeMillis();
        joinOutput.setStartTimeMs(startTime);
        joinOutput.setRequestId(context.getRequestId());
        joinOutput.setSuccessful(true);
        joinOutput.setErrorMessage("");

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            requireNonNull(event.getSmallTable(), "event.smallTable is null");
            StorageInfo leftInputStorageInfo = event.getSmallTable().getStorageInfo();
            List<String> leftSorted = event.getSmallTable().getInputFiles();
            requireNonNull(leftSorted, "leftSorted is null");
            checkArgument(!leftSorted.isEmpty(), "leftSorted is empty");
            int leftParallelism = event.getSmallTable().getParallelism();
            checkArgument(leftParallelism > 0, "leftParallelism is not positive");
            String[] leftColumnsToRead = event.getSmallTable().getColumnsToRead();
            int[] leftKeyColumnIds = event.getSmallTable().getKeyColumnIds();

            requireNonNull(event.getLargeTable(), "event.largeTable is null");
            StorageInfo rightInputStorageInfo = event.getLargeTable().getStorageInfo();
            List<String> rightSorted = event.getLargeTable().getInputFiles();
            requireNonNull(rightSorted, "rightSorted is null");
            checkArgument(!rightSorted.isEmpty(), "rightSorted is empty");
            int rightParallelism = event.getLargeTable().getParallelism();
            checkArgument(rightParallelism > 0, "rightParallelism is not positive");
            String[] rightColumnsToRead = event.getLargeTable().getColumnsToRead();
            int[] rightKeyColumnIds = event.getLargeTable().getKeyColumnIds();

            String[] leftColAlias = event.getJoinInfo().getSmallColumnAlias();
            String[] rightColAlias = event.getJoinInfo().getLargeColumnAlias();
            boolean[] leftProjection = event.getJoinInfo().getSmallProjection();
            boolean[] rightProjection = event.getJoinInfo().getLargeProjection();
            JoinType joinType = event.getJoinInfo().getJoinType();
            logger.info("small table: " + event.getSmallTable().getTableName() +
                    ", large table: " + event.getLargeTable().getTableName());

            MultiOutputInfo outputInfo = event.getOutput();
            StorageInfo outputStorageInfo = outputInfo.getStorageInfo();
            if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
            {
                checkArgument(outputInfo.getFileNames().size() == 2,
                        "it is incorrect to have more than two output files");
            } else
            {
                checkArgument(outputInfo.getFileNames().size() == 1,
                        "it is incorrect to have more than one output files");
            }
            String outputFolder = outputInfo.getPath();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = outputInfo.isEncoding();

            WorkerCommon.initStorage(leftInputStorageInfo);
            WorkerCommon.initStorage(rightInputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            WorkerCommon.getFileSchemaFromPaths(threadPool,
                    WorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                    WorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                    leftSchema, rightSchema, leftSorted, rightSorted);

            SortedJoiner joiner = new SortedJoiner(joinType,
                    WorkerCommon.getResultSchema(leftSchema.get(), leftColumnsToRead),
                    leftColAlias, leftProjection, leftKeyColumnIds,
                    WorkerCommon.getResultSchema(rightSchema.get(), rightColumnsToRead),
                    rightColAlias, rightProjection, rightKeyColumnIds);

            List<Future> leftFutures = new ArrayList<>(leftSorted.size());
            int leftSplitSize = leftSorted.size();
            if (leftSorted.size() % leftParallelism > 0)
            {
                leftSplitSize++;
            }
            for (int i = 0; i < leftSorted.size(); i += leftSplitSize)
            {
                List<String> parts = new LinkedList<>();
                for (int j = i; j < i + leftSplitSize && j < leftSorted.size(); ++j)
                {
                    parts.add(leftSorted.get(j));
                }
                final int partIndex = i;
                leftFutures.add(threadPool.submit(() -> {
                    try
                    {
                        addLeftTable(transId, joiner, parts, partIndex, leftColumnsToRead, leftInputStorageInfo.getScheme(), workerMetrics);
                    } catch (Throwable e)
                    {
                        throw new WorkerException("error during hash table construction", e);
                    }
                }));
            }
            for (Future future : leftFutures)
            {
                future.get();
            }
            joiner.mergeLeftTable();

            logger.info("duration (ns): " +
                    (workerMetrics.getInputCostNs() + workerMetrics.getComputeCostNs()));
            ConcurrentLinkedQueue<VectorizedRowBatch> result = new ConcurrentLinkedQueue<>();


            // scan the right table and do the join.
            if (!joiner.sortedSmallTable.isEmpty())
            {
                int rightSplitSize = rightSorted.size() / rightParallelism;
                if (rightSorted.size() % rightParallelism > 0)
                {
                    rightSplitSize++;
                }

                for (int i = 0; i < rightSorted.size(); i += rightSplitSize)
                {
                    List<String> parts = new LinkedList<>();
                    for (int j = i; j < i + rightSplitSize && j < rightSorted.size(); ++j)
                    {
                        parts.add(rightSorted.get(j));
                    }
                    threadPool.execute(() -> {
                        try
                        {
                            int numJoinedRows =
                                    joinWithRightTable(transId, joiner, parts, rightColumnsToRead,
                                            rightInputStorageInfo.getScheme(),
                                            result, workerMetrics);
                        } catch (Throwable e)
                        {
                            throw new WorkerException("error during hash join", e);
                        }
                    });
                }
                threadPool.shutdown();
                try
                {
                    while (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) ;
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the termination of join", e);
                }

                if (exceptionHandler.hasException())
                {
                    throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
                }
            }

            String outputPath = outputFolder + outputInfo.getFileNames().get(0);
            try
            {
                WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                PixelsWriter pixelsWriter;
                pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                        WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                        encoding, false, null);
                ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result;
                for (VectorizedRowBatch rowBatch : rowBatches)
                {
                    pixelsWriter.addRowBatch(rowBatch);
                }
                pixelsWriter.close();
                workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }

                if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
                {
                    // output the left-outer tail.
                    outputPath = outputFolder + outputInfo.getFileNames().get(1);
                    pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                            WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                            encoding, false, null);
                    joiner.writeLeftOuter(pixelsWriter, WorkerCommon.rowBatchSize);
                    pixelsWriter.close();
                    workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                    workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                    joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                    if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                    {
                        while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                        {
                            // Wait for 10ms and see if the output file is visible.
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                }
                workerMetrics.addOutputCostNs(writeCostTimer.stop());
            } catch (Throwable e)
            {
                throw new WorkerException(
                        "failed to finish writing and close the join result file '" + outputPath + "'", e);
            }

            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(joinOutput, workerMetrics);
            return joinOutput;
        } catch (Throwable e)
        {
            logger.error("error during join", e);
            joinOutput.setSuccessful(false);
            joinOutput.setErrorMessage(e.getMessage());
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return joinOutput;
        }
    }

    /**
     * Sort left table and populate this table for the join.
     *
     * @param transId       the transaction id used by I/O scheduler
     * @param joiner        the joiner for which the sorted table is built
     * @param leftParts     the information of Sorted files of the left table
     * @param leftCols      the column names of the left table
     * @param leftScheme    the storage scheme of the left table
     * @param workerMetrics the collector of the performance metrics
     */
    protected static void addLeftTable(long transId, SortedJoiner joiner, List<String> leftParts, int partIndex, String[] leftCols,
                                       Storage.Scheme leftScheme,
                                       WorkerMetrics workerMetrics)
    {
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!leftParts.isEmpty())
        {
            for (Iterator<String> it = leftParts.iterator(); it.hasNext(); )
            {
                String leftSorted = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        leftSorted, WorkerCommon.getStorage(leftScheme)))
                {
                    readCostTimer.stop();
                    PixelsReaderOption option = WorkerCommon.getReaderOption(transId, leftCols);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");

                    computeCostTimer.start();
                    do
                    {
                        rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                        if (rowBatch.size > 0)
                        {
                            joiner.populateLeftTable(rowBatch, partIndex);
                        }
                    } while (!rowBatch.endOfFile);

                    computeCostTimer.stop();
                    computeCostTimer.minus(recordReader.getReadTimeNanos());
                    readCostTimer.add(recordReader.getReadTimeNanos());
                    readBytes += recordReader.getCompletedBytes();
                    numReadRequests += recordReader.getNumReadRequests();
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the Sorted file '" +
                            leftSorted + "' and build the sorted table", e);
                }
            }
            if (!leftParts.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the Sorted files");
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
    }


    /**
     * Scan the Sorted file of the right table and do the join.
     *
     * @param transId       the transaction id used by I/O scheduler
     * @param joiner        the joiner for the Sorted join
     * @param rightParts    the information of Sorted files of the right table
     * @param rightCols     the column names of the right table
     * @param rightScheme   the storage scheme of the right table
     * @param joinResult    the container of the join result
     * @param workerMetrics the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    protected static int joinWithRightTable(
            long transId, Joiner joiner, List<String> rightParts, String[] rightCols, Storage.Scheme rightScheme,
            ConcurrentLinkedQueue<VectorizedRowBatch> joinResult,
            WorkerMetrics workerMetrics)
    {
        int joinedRows = 0;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightSorted = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        rightSorted, WorkerCommon.getStorage(rightScheme)))
                {
                    readCostTimer.stop();
                    PixelsReaderOption option = WorkerCommon.getReaderOption(transId, rightCols);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");
                    computeCostTimer.start();
                    do
                    {
                        rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                        if (rowBatch.size > 0)
                        {
                            List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                            for (VectorizedRowBatch joined : joinedBatches)
                            {
                                joinResult.add(joined);
                                joinedRows += joined.size;
                            }
                        }
                    } while (!rowBatch.endOfFile);
                    computeCostTimer.stop();
                    computeCostTimer.minus(recordReader.getReadTimeNanos());
                    readCostTimer.add(recordReader.getReadTimeNanos());
                    readBytes += recordReader.getCompletedBytes();
                    numReadRequests += recordReader.getNumReadRequests();
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the Sorted file '" +
                            rightSorted + "' and do the join", e);
                }
            }
            if (!rightParts.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the Sorted files");
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        return joinedRows;
    }

}
