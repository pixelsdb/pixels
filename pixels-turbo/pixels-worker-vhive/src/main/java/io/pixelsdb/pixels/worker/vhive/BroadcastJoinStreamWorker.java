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
package io.pixelsdb.pixels.worker.vhive;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.executor.join.Partitioner;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateService;
import io.pixelsdb.pixels.planner.plan.logical.Join;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.*;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BroadcastJoinStreamWorker extends BaseBroadcastJoinWorker implements RequestHandler<BroadcastJoinInput, JoinOutput> {
    private final Logger logger;
    protected WorkerCoordinateService workerCoordinatorService;
    private final WorkerMetrics workerMetrics;
    private io.pixelsdb.pixels.common.task.Worker<CFWorkerInfo> worker;

    public BroadcastJoinStreamWorker(WorkerContext context) {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    @Override
    public JoinOutput handleRequest(BroadcastJoinInput input) {
        long startTime = System.currentTimeMillis();
        try {
            int stageId = input.getStageId();
            long transId = input.getTransId();
            String ip = WorkerCommon.getIpAddress();
            int port = WorkerCommon.getPort();
            String coordinatorIp = WorkerCommon.getCoordinatorIp();
            int coordinatorPort = WorkerCommon.getCoordinatorPort();
            CFWorkerInfo workerInfo = new CFWorkerInfo(ip, port, transId, stageId, "broadcast_join", Collections.emptyList());
            workerCoordinatorService = new WorkerCoordinateService(coordinatorIp, coordinatorPort);
            worker = workerCoordinatorService.registerWorker(workerInfo);
            JoinOutput output = process(input);
            workerCoordinatorService.terminateWorker(worker.getWorkerId());
            return output;
        } catch (Throwable e) {
            JoinOutput joinOutput = new JoinOutput();
            this.logger.error("error during registering worker", e);
            joinOutput.setSuccessful(false);
            joinOutput.setErrorMessage(e.getMessage());
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return joinOutput;
        }
    }

    @Override
    public String getRequestId() {
        return this.context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType() {
        return WorkerType.BROADCAST_JOIN_STREAMING;
    }

    @Override
    public JoinOutput process(BroadcastJoinInput input) {
        JoinOutput joinOutput = new JoinOutput();
        long startTime = System.currentTimeMillis();
        joinOutput.setStartTimeMs(startTime);
        joinOutput.setRequestId(context.getRequestId());
        joinOutput.setSuccessful(true);
        joinOutput.setErrorMessage("");

        try {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = input.getTransId();
            BroadcastTableInfo leftTable = requireNonNull(input.getSmallTable(), "leftTable is null");
            StorageInfo leftInputStorageInfo = requireNonNull(leftTable.getStorageInfo(), "leftStorageInfo is null");
            List<InputSplit> leftInputs = requireNonNull(leftTable.getInputSplits(), "leftInputs is null");
            checkArgument(leftInputs.size() > 0, "left table is empty");
            String[] leftColumnsToRead = leftTable.getColumnsToRead();
            int[] leftKeyColumnIds = leftTable.getKeyColumnIds();
            TableScanFilter leftFilter = JSON.parseObject(leftTable.getFilter(), TableScanFilter.class);

            BroadcastTableInfo rightTable = requireNonNull(input.getLargeTable(), "rightTable is null");
            StorageInfo rightInputStorageInfo = requireNonNull(rightTable.getStorageInfo(), "rightStorageInfo is null");
            List<InputSplit> rightInputs = requireNonNull(rightTable.getInputSplits(), "rightInputs is null");
            checkArgument(rightInputs.size() > 0, "right table is empty");
            String[] rightColumnsToRead = rightTable.getColumnsToRead();
            int[] rightKeyColumnIds = rightTable.getKeyColumnIds();
            TableScanFilter rightFilter = JSON.parseObject(rightTable.getFilter(), TableScanFilter.class);

            String[] leftColAlias = input.getJoinInfo().getSmallColumnAlias();
            String[] rightColAlias = input.getJoinInfo().getLargeColumnAlias();
            boolean[] leftProjection = input.getJoinInfo().getSmallProjection();
            boolean[] rightProjection = input.getJoinInfo().getLargeProjection();
            JoinType joinType = input.getJoinInfo().getJoinType();
            checkArgument(joinType != JoinType.EQUI_LEFT && joinType != JoinType.EQUI_FULL,
                    "broadcast join can not be used for LEFT_OUTER or FULL_OUTER join");

            MultiOutputInfo outputInfo = input.getOutput();
            StorageInfo outputStorageInfo = outputInfo.getStorageInfo();
            checkArgument(outputInfo.getFileNames().size() == 1,
                    "it is incorrect to have more than one output files");
            String outputFolder = outputInfo.getPath();
            if (!outputFolder.endsWith("/")) {
                outputFolder += "/";
            }
            boolean encoding = outputInfo.isEncoding();

            StreamWorkerCommon.initStorage(leftInputStorageInfo);
            StreamWorkerCommon.initStorage(rightInputStorageInfo);
            StreamWorkerCommon.initStorage(outputStorageInfo);

            List<String> rightEndpoints = Arrays.asList("http://localhost:18686/", "http://localhost:18687/", "http://localhost:18688/", "http://localhost:18689/");
            boolean partitionOutput = input.getJoinInfo().isPostPartition();
            PartitionInfo outputPartitionInfo = input.getJoinInfo().getPostPartitionInfo();
            if (partitionOutput) {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }

            // build the joiner
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            if (leftInputStorageInfo.getScheme() == Storage.Scheme.httpstream && rightInputStorageInfo.getScheme() == Storage.Scheme.httpstream) {
                StreamWorkerCommon.getSchemaFromPaths(threadPool,
                        StreamWorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                        StreamWorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                        leftSchema, rightSchema,
                        Collections.singletonList("http://localhost:18688/"),
                        Collections.singletonList("http://localhost:18686/"));
            } else if (leftInputStorageInfo.getScheme() != Storage.Scheme.httpstream && rightInputStorageInfo.getScheme() == Storage.Scheme.httpstream) {
                StreamWorkerCommon.getSchemaFromTwoPaths(threadPool,
                        StreamWorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                        StreamWorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                        leftSchema, rightSchema,
                        leftInputs,
                        rightEndpoints);
            } else if (leftInputStorageInfo.getScheme() != Storage.Scheme.httpstream && rightInputStorageInfo.getScheme() != Storage.Scheme.httpstream) {
                WorkerCommon.getFileSchemaFromSplits(threadPool,
                        StreamWorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                        StreamWorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                        leftSchema, rightSchema, leftInputs, rightInputs);
            }
            Joiner joiner = new Joiner(joinType,
                    StreamWorkerCommon.getResultSchema(leftSchema.get(), leftColumnsToRead),
                    leftColAlias, leftProjection, leftKeyColumnIds,
                    StreamWorkerCommon.getResultSchema(rightSchema.get(), rightColumnsToRead),
                    rightColAlias, rightProjection, rightKeyColumnIds);
            logger.info("succeed to get left and right schema");

            // build the hash table for the left table.
            List<Future> leftFutures = new ArrayList<>();
            for (InputSplit inputSplit : leftInputs) {
                List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                leftFutures.add(threadPool.submit(() -> {
                    try {
                        buildHashTable(transId, joiner, inputs, leftInputStorageInfo.getScheme(),
                                !leftTable.isBase(), leftColumnsToRead, leftFilter, workerMetrics);
                    } catch (Throwable e) {
                        throw new WorkerException("error during hash table construction", e);
                    }
                }));
            }
            for (Future future : leftFutures) {
                future.get();
            }
            logger.info("succeed to build hash table");

            List<ConcurrentLinkedQueue<VectorizedRowBatch>> result = new ArrayList<>();
            if (partitionOutput) {
                for (int i = 0; i < outputPartitionInfo.getNumPartition(); ++i) {
                    result.add(new ConcurrentLinkedQueue<>());
                }
            } else {
                result.add(new ConcurrentLinkedQueue<>());
            }

            // scan the right table and do the join.
            if (joiner.getSmallTableSize() > 0)
            {
                if (rightInputStorageInfo.getScheme() == Storage.Scheme.httpstream)
                {
                    logger.info("scan right input from http");
                    for (String endpoint : rightEndpoints)
                    {
                        threadPool.execute(() -> {
                            try
                            {
                                if (partitionOutput)
                                {
                                    throw new UnsupportedOperationException("don't support partitioning operation");
                                } else
                                {
                                    joinWithRightTable(transId, joiner, endpoint, rightInputStorageInfo.getScheme(), rightColumnsToRead,
                                            rightFilter, result.get(0), workerMetrics, logger);
                                }
                            } catch (Throwable e)
                            {
                                throw new WorkerException("error during broadcast join", e);
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
                } else
                {
                    logger.info("scan right input from non http");
                    for (InputSplit inputSplit : rightInputs)
                    {
                        List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                        threadPool.execute(() -> {
                            try
                            {
                                int numJoinedRows = partitionOutput ?
                                        joinWithRightTableAndPartition(
                                                transId, joiner, inputs, rightInputStorageInfo.getScheme(),
                                                !rightTable.isBase(), rightColumnsToRead, rightFilter,
                                                outputPartitionInfo, result, workerMetrics) :
                                        joinWithRightTable(transId, joiner, inputs, rightInputStorageInfo.getScheme(),
                                                !rightTable.isBase(), rightColumnsToRead, rightFilter, result.get(0), workerMetrics);
                            } catch (Throwable e)
                            {
                                throw new WorkerException("error during broadcast join", e);
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
            }
            logger.info("succeed to join with right table");

            String outputPath = outputFolder + outputInfo.getFileNames().get(0);
            List<CFWorkerInfo> downStreamWorkers = workerCoordinatorService.getDownstreamWorkers(worker.getWorkerId())
                    .stream()
                    .sorted(Comparator.comparing(worker -> worker.getHashValues().get(0)))
                    .collect(ImmutableList.toImmutableList());
            List<String> outputEndpoints = downStreamWorkers.stream()
                    .map(CFWorkerInfo::getIp)
                    .map(ip -> "http://" + ip + ":"
                            + (18686 + worker.getWorkerPortIndex()) + "/")
                    // .map(URI::create)
                    .collect(Collectors.toList());

            logger.info("down stream workers are {}", downStreamWorkers);
            if (!downStreamWorkers.isEmpty()) {
                StreamWorkerCommon.passSchemaToNextLevel(joiner.getJoinedSchema(), outputStorageInfo,
                        outputEndpoints);
                logger.info("succeed to pass schema to next level");
                try {
                    WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                    PixelsWriter pixelsWriter;
                    if (partitionOutput) {
                        pixelsWriter = StreamWorkerCommon.getWriter(joiner.getJoinedSchema(),
                                StreamWorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath, encoding,
                                true, 0,
                                Arrays.stream(leftKeyColumnIds).boxed().collect(Collectors.toList()),
                                outputEndpoints, false);
                        for (int hash = 0; hash < outputPartitionInfo.getNumPartition(); ++hash) {
                            ConcurrentLinkedQueue<VectorizedRowBatch> batches = result.get(hash);
                            if (!batches.isEmpty()) {
                                for (VectorizedRowBatch batch : batches) {
                                    pixelsWriter.addRowBatch(batch, hash);
                                }
                            }
                        }
                    } else {
                        pixelsWriter = StreamWorkerCommon.getWriter(joiner.getJoinedSchema(),
                                StreamWorkerCommon.getStorage(outputStorageInfo.getScheme()),
                                outputEndpoints.get(0), encoding,
                                false, 0,
                                null, null, false);
                        ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result.get(0);
                        for (VectorizedRowBatch rowBatch : rowBatches) {
                            pixelsWriter.addRowBatch(rowBatch);
                        }
                    }
                    pixelsWriter.close();
                    logger.info("succeed to write results");
                    joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                    if (outputStorageInfo.getScheme() == Storage.Scheme.minio) {
                        while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath)) {
                            // Wait for 10ms and see if the output file is visible.
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                    workerMetrics.addOutputCostNs(writeCostTimer.stop());
                    workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                    workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                } catch (Throwable e) {
                    throw new WorkerException(
                            "failed to finish writing and close the join result file '" + outputPath + "'", e);
                }
            } else {
                try {
                    PixelsWriter pixelsWriter;
                    WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                    if (partitionOutput) {
                        pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                                WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                                encoding, true, Arrays.stream(
                                                outputPartitionInfo.getKeyColumnIds()).boxed().
                                        collect(Collectors.toList()));
                        for (int hash = 0; hash < outputPartitionInfo.getNumPartition(); ++hash) {
                            ConcurrentLinkedQueue<VectorizedRowBatch> batches = result.get(hash);
                            if (!batches.isEmpty()) {
                                for (VectorizedRowBatch batch : batches) {
                                    pixelsWriter.addRowBatch(batch, hash);
                                }
                            }
                        }
                    } else {
                        pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                                WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                                encoding, false, null);
                        ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result.get(0);
                        for (VectorizedRowBatch rowBatch : rowBatches) {
                            pixelsWriter.addRowBatch(rowBatch);
                        }
                    }
                    pixelsWriter.close();
                    joinOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
                    if (outputStorageInfo.getScheme() == Storage.Scheme.minio) {
                        while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath)) {
                            // Wait for 10ms and see if the output file is visible.
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                    workerMetrics.addOutputCostNs(writeCostTimer.stop());
                    workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                    workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                } catch (Throwable e) {
                    throw new WorkerException(
                            "failed to finish writing and close the join result file '" + outputPath + "'", e);
                }
            }
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(joinOutput, workerMetrics);
            return joinOutput;
        } catch (Throwable e) {
            logger.error("error during join", e);
            joinOutput.setSuccessful(false);
            joinOutput.setErrorMessage(e.getMessage());
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return joinOutput;
        }
    }

    /**
     * Scan the input files of the right table and do the join.
     *
     * @param transId        the transaction id used by I/O scheduler
     * @param joiner         the joiner for the broadcast join
     * @param rightScheme    the storage scheme of the right table
     * @param rightCols      the column names of the right table
     * @param rightFilter    the table scan filter on the right table
     * @param joinResult     the container of the join result
     * @param workerMetrics  the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    public static int joinWithRightTable(
            long transId, Joiner joiner, String rightEndpoint,
            Storage.Scheme rightScheme, String[] rightCols, TableScanFilter rightFilter,
            ConcurrentLinkedQueue<VectorizedRowBatch> joinResult, WorkerMetrics workerMetrics, Logger logger) {
        logger.info("join with right table endpoint {}", rightEndpoint);
        int joinedRows = 0;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;

        readCostTimer.start();
        PixelsReader pixelsReader;
        try
        {
            pixelsReader = StreamWorkerCommon.getReader(rightScheme, rightEndpoint);
            readCostTimer.stop();
            PixelsReaderOption option = StreamWorkerCommon.getReaderOption(transId, rightCols);
            PixelsRecordReader recordReader = pixelsReader.read(option);
            VectorizedRowBatch rowBatch;

            Bitmap filtered = new Bitmap(StreamWorkerCommon.rowBatchSize, true);
            Bitmap tmp = new Bitmap(StreamWorkerCommon.rowBatchSize, false);
            computeCostTimer.start();
            do
            {
                rowBatch = recordReader.readBatch(StreamWorkerCommon.rowBatchSize);
//                logger.info("record reader read row batch size before filter {}", rowBatch.size);
                rightFilter.doFilter(rowBatch, filtered, tmp);
                rowBatch.applyFilter(filtered);
//                logger.info("record reader read row batch size after filter {}", rowBatch.size);
                if (rowBatch.size > 0) {
                    logger.info("row batch size > 0");
                    List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                    for (VectorizedRowBatch joined : joinedBatches) {
                        if (!joined.isEmpty()) {
                            logger.info("joined result add {}", joined.size);
                            joinResult.add(joined);
                            joinedRows += joined.size;
                        }
                    }
                }
            } while (!rowBatch.endOfFile);
            pixelsReader.close();
            computeCostTimer.stop();
            computeCostTimer.minus(recordReader.getReadTimeNanos());
            readCostTimer.add(recordReader.getReadTimeNanos());
            readBytes += recordReader.getCompletedBytes();
            numReadRequests += recordReader.getNumReadRequests();
        } catch (Throwable e) {
            throw new WorkerException("failed to scan the right table input file '" +
                    rightEndpoint + "' and do the join", e);
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        return joinedRows;
    }
}