package io.pixelsdb.pixels.worker.vhive;

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
import io.pixelsdb.pixels.worker.common.*;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Implemented c.f. {@link io.pixelsdb.pixels.worker.common.BaseAggregationWorker}.
 *
 * @author jasha64
 * @create 2023-09-04
 */
public class BaseAggregationStreamWorker extends Worker<AggregationInput, AggregationOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseAggregationStreamWorker(WorkerContext context)
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

        try {
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
            if (inputPartitioned) {
                hashValues = requireNonNull(aggregationInfo.getHashValues(),
                        "aggregationInfo.hashValues is null");
                checkArgument(!hashValues.isEmpty(), "aggregationInfo.hashValues is empty");
            } else {
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
                    "aggregationInfo.groupKeyColumnNames is null");
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

            StreamWorkerCommon.initStorage(inputStorageInfo);
            StreamWorkerCommon.initStorage(outputStorageInfo);

            logger.info("start get output schema");
            TypeDescription fileSchema = StreamWorkerCommon.getSchemaFromPaths(
                    StreamWorkerCommon.getStorage(inputStorageInfo.getScheme()), inputFiles);
            logger.debug("getSchemaFromPaths result: " + fileSchema.getChildren());
            checkArgument(fileSchema.getChildren().size() == columnsToRead.length,
                    "input file does not contain the correct number of columns");
            TypeDescription inputSchema = StreamWorkerCommon.getResultSchema(fileSchema, columnsToRead);
            // StreamWorkerCommon.passSchemaToNextLevel(inputSchema, outputStorageInfo);  // In demo, the agg worker is the last worker, so no need to pass schema to next level.

            logger.debug("start aggregation");
            // In the legacy workers, they mostly use logger.info for I/O operations - see BaseScanWorker.
            // So we only use logger.debug here.
            for (int i = 0; i < functionTypes.length; ++i)
            {
                if (functionTypes[i] == FunctionType.COUNT)
                {
                    functionTypes[i] = FunctionType.SUM;
                }
            }
            Aggregator aggregator = new Aggregator(StreamWorkerCommon.rowBatchSize, inputSchema, groupKeyColumnNames,
                    groupKeyColumnIds, groupKeyColumnProj, aggrColumnIds, resultColumnNames,
                    resultColumnTypes, functionTypes, false, 0);
            for (int i = 0; i < inputFiles.size(); )
            {
                List<String> files = new LinkedList<>();
                for (int j = 0; j < parallelism && i < inputFiles.size(); ++j, ++i)
                {
                    logger.debug("try aggregation for file " + i + ": " + inputFiles.get(i));
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
                        e.printStackTrace();
                        throw new WorkerException("error during aggregation", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                e.printStackTrace();
                throw new WorkerException("interrupted while waiting for the termination of aggregation", e);
            }

            if (exceptionHandler.hasException())
            {
                throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
            }

            WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
            logger.debug("creating a new pixelsWriter on endpoint " + outputPath);
            PixelsWriter pixelsWriter = StreamWorkerCommon.getWriter(aggregator.getOutputSchema(),
                    StreamWorkerCommon.getStorage(outputStorageInfo.getScheme()),
                    outputPath, encoding);
            aggregator.writeAggrOutput(pixelsWriter);
            pixelsWriter.close();

            workerMetrics.addOutputCostNs(writeCostTimer.stop());
            workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
            workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
            aggregationOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
            aggregationOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            StreamWorkerCommon.setPerfMetrics(aggregationOutput, workerMetrics);
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

    private int aggregate(long transId, List<String> inputFiles, String[] columnsToRead,
                          Storage.Scheme inputScheme, List<Integer> hashValues,
                          int numPartition, Aggregator aggregator, WorkerMetrics workerMetrics) throws IOException {
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
                logger.debug("creating a new pixels stream Reader on endpoint " + inputFile);
                readCostTimer.start();
                PixelsReader pixelsReader = null;
                try
                {
                    pixelsReader = StreamWorkerCommon.getReader(inputScheme, inputFile);
                    readCostTimer.stop();
                    // if (pixelsReader.getRowGroupNum() == 0)
                    // {
                    //     it.remove();
                    //     continue;
                    // }
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
                            rowBatch = recordReader.readBatch(StreamWorkerCommon.rowBatchSize);
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
                        // Streaming mode does not support getting existent hash values of all row groups in advance.
                        // (Or alternatively, maybe send necessary information in the header but summarize the hash values in the footer?) Therefore,
                        //  we will just pass each hash value to the reader and let it decide whether to read.
                        // Set<Integer> existHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                        // for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                        // {
                        //     existHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                        // }
                        for (int hashValue : hashValues)
                        {
                            // if (!existHashValues.contains(hashValue))
                            // {
                            //     continue;
                            // }
                            PixelsReaderOption option = StreamWorkerCommon.getReaderOption(transId, columnsToRead, pixelsReader,
                                    hashValue, numPartition);
                            PixelsRecordReader recordReader = pixelsReader.read(option);
                            if (recordReader == null) continue;
                            VectorizedRowBatch rowBatch;

                            computeCostTimer.start();
                            do
                            {
                                rowBatch = recordReader.readBatch(StreamWorkerCommon.rowBatchSize);
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
                            // recordReader will be automatically closed when closing pixelsReader.
                        }
                    }
                    it.remove();
                }
                catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to read the input partial aggregation file '" +
                            inputFile + "' and perform aggregation", e);
                }
                finally {
                    if (pixelsReader != null) {
                        pixelsReader.close();
                    }
                }
            }
            if (!inputFiles.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
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
