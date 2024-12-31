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
package io.pixelsdb.pixels.invoker.lambda.mock;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.executor.join.SortedJoiner;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.SortedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MockSortedJoinWorker
{
    private final Logger logger = LogManager.getLogger(MockSortedJoinWorker.class);

    public MockSortedJoinWorker()
    {
    }

    public JoinOutput process(SortedJoinInput event)
    {
        JoinOutput joinOutput = new JoinOutput();
        long startTime = System.currentTimeMillis();
        joinOutput.setStartTimeMs(startTime);
        joinOutput.setRequestId("123456");
        joinOutput.setSuccessful(true);
        joinOutput.setErrorMessage("");

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);

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

            MockWorkerCommon.initStorage(leftInputStorageInfo);
            MockWorkerCommon.initStorage(rightInputStorageInfo);
            MockWorkerCommon.initStorage(outputStorageInfo);

            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            MockWorkerCommon.getFileSchemaFromPaths(threadPool,
                    MockWorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                    MockWorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                    leftSchema, rightSchema, leftSorted, rightSorted);
            /*
             * Issue #450:
             * For the left and the right partial Sorted files, the file schema is equal to the columns to read in normal cases.
             * However, it is safer to turn file schema into result schema here.
             */
            SortedJoiner joiner = new SortedJoiner(joinType,
                    MockWorkerCommon.getResultSchema(leftSchema.get(), leftColumnsToRead),
                    leftColAlias, leftProjection, leftKeyColumnIds,
                    MockWorkerCommon.getResultSchema(rightSchema.get(), rightColumnsToRead),
                    rightColAlias, rightProjection, rightKeyColumnIds);

            List<Future> leftFutures = new ArrayList<>(leftSorted.size());
            int leftSplitSize = leftSorted.size();// 文件的数量 / leftParallelism; // 1个线程要处理的文件数量
            if (leftSorted.size() % leftParallelism > 0)
            {
                leftSplitSize++;
            }
            // left table要并行sort有办法
            // 划分后sort。以3线程为例， sort要创建三个容器，三个线程，同时往容器里放数据。 然后等放完之后，分别sort。 等到三个容器都sort完，再进行merge
            // （增加方法，mergeLeftTable）
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
                        addLeftTable(transId, joiner, parts, partIndex, leftColumnsToRead, leftInputStorageInfo.getScheme());
                    } catch (Throwable e)
                    {
                        throw new RuntimeException("error during hash table construction", e);
                    }
                }));
            }
            for (Future future : leftFutures)
            {
                future.get();
            }
            joiner.mergeLeftTable();

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
                                            result);
                            System.out.println("numJoinedRows = " + numJoinedRows);
                        } catch (Throwable e)
                        {
                            throw new RuntimeException("error during sort join", e); // 改注释
                        }
                    });
                }
                threadPool.shutdown();
                try
                {
                    while (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) ;
                } catch (InterruptedException e)
                {
                    throw new RuntimeException("interrupted while waiting for the termination of join", e);
                }

            }

            String outputPath = outputFolder + outputInfo.getFileNames().get(0);
            try
            {
                PixelsWriter pixelsWriter;
                pixelsWriter = MockWorkerCommon.getWriter(joiner.getJoinedSchema(),
                        MockWorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                        encoding, false, null);
                ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result;
                for (VectorizedRowBatch rowBatch : rowBatches)
                {
                    pixelsWriter.addRowBatch(rowBatch);
                }
                pixelsWriter.close();
                joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!MockWorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }

                if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
                {
                    // output the left-outer tail.
                    outputPath = outputFolder + outputInfo.getFileNames().get(1);
                    pixelsWriter = MockWorkerCommon.getWriter(joiner.getJoinedSchema(),
                            MockWorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                            encoding, false, null);
                    joiner.writeLeftOuter(pixelsWriter, MockWorkerCommon.rowBatchSize);
                    pixelsWriter.close();
                    joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                    if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                    {
                        while (!MockWorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                        {
                            // Wait for 10ms and see if the output file is visible.
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                }
            } catch (Throwable e)
            {
                throw new RuntimeException(
                        "failed to finish writing and close the join result file '" + outputPath + "'", e);
            }

            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
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

    protected static void addLeftTable(long transId, SortedJoiner joiner, List<String> leftParts, int partIndex, String[] leftCols,
                                       Storage.Scheme leftScheme)
    {
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!leftParts.isEmpty())
        {
            for (Iterator<String> it = leftParts.iterator(); it.hasNext(); )
            {
                String leftSorted = it.next();
                try (PixelsReader pixelsReader = MockWorkerCommon.getReader(
                        leftSorted, MockWorkerCommon.getStorage(leftScheme)))
                {
                    PixelsReaderOption option = MockWorkerCommon.getReaderOption(transId, leftCols);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");

                    do
                    {
                        rowBatch = recordReader.readBatch(MockWorkerCommon.rowBatchSize);
                        if (rowBatch.size > 0)
                        {
                            joiner.populateLeftTable(rowBatch, partIndex);
                        }
                    } while (!rowBatch.endOfFile);

                    readBytes += recordReader.getCompletedBytes();
                    numReadRequests += recordReader.getNumReadRequests();
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new RuntimeException("failed to scan the Sorted file '" +
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
                    throw new RuntimeException("interrupted while waiting for the Sorted files");
                }
            }
        }
    }

    protected static int joinWithRightTable(
            long transId, Joiner joiner, List<String> rightParts, String[] rightCols, Storage.Scheme rightScheme,
            ConcurrentLinkedQueue<VectorizedRowBatch> joinResult)
    {
        int joinedRows = 0;
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightSorted = it.next();
                try (PixelsReader pixelsReader = MockWorkerCommon.getReader(
                        rightSorted, MockWorkerCommon.getStorage(rightScheme)))
                {
                    PixelsReaderOption option = MockWorkerCommon.getReaderOption(transId, rightCols);
                    VectorizedRowBatch rowBatch;
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    checkArgument(recordReader.isValid(), "failed to get record reader");

                    do
                    {
                        rowBatch = recordReader.readBatch(MockWorkerCommon.rowBatchSize);
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
                    readBytes += recordReader.getCompletedBytes();
                    numReadRequests += recordReader.getNumReadRequests();
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new RuntimeException("failed to scan the Sorted file '" +
                            rightSorted + "' and do the join", e); // 改注释
                }
            }
            if (!rightParts.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new RuntimeException("interrupted while waiting for the Sorted files");
                }
            }
        }
        return joinedRows;
    }
}
