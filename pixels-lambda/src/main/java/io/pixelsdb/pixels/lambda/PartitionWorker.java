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
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.Partitioner;
import io.pixelsdb.pixels.executor.lambda.domain.InputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.InputSplit;
import io.pixelsdb.pixels.executor.lambda.input.PartitionInput;
import io.pixelsdb.pixels.executor.lambda.output.PartitionOutput;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;

/**
 * @author hank
 * @date 07/05/2022
 */
public class PartitionWorker implements RequestHandler<PartitionInput, PartitionOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionWorker.class);

    @Override
    public PartitionOutput handleRequest(PartitionInput event, Context context)
    {
        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);

            long queryId = event.getQueryId();
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            int numPartition = event.getPartitionInfo().getNumParition();
            logger.info("table '" + event.getTableInfo().getTableName() +
                    "', number of partitions (" + numPartition + ")");
            int[] keyColumnIds = event.getPartitionInfo().getKeyColumnIds();
            checkArgument(event.getOutput().getScheme() == Storage.Scheme.s3,
                    "the storage scheme for the partition result must be s3");
            String outputPath = event.getOutput().getPath();
            boolean encoding = event.getOutput().isEncoding();

            String[] cols = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);
            PartitionOutput partitionOutput = new PartitionOutput();
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
                        partitionFile(queryId, scanInputs, cols, filter,
                                keyColumnIds, partitioned, writerSchema);
                    }
                    catch (Exception e)
                    {
                        logger.error("error during partitioning", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                logger.error("interrupted while waiting for the termination of partitioning", e);
            }

            PixelsWriter pixelsWriter = getWriter(writerSchema.get(), s3, outputPath, encoding,
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
            partitionOutput.setPath(outputPath);
            partitionOutput.setHashValues(hashValues);

            pixelsWriter.close();

            return partitionOutput;
        }
        catch (Exception e)
        {
            logger.error("error during partition", e);
            return null;
        }
    }

    /**
     * Scan and partition the files in a query split.
     *
     * @param queryId the query id used by I/O scheduler
     * @param scanInputs the information of the files to scan
     * @param cols the included columns
     * @param filter the filer for the scan
     * @param keyColumnIds the ids of the partition key columns
     * @param partitionResult the partition result
     * @param writerSchema the schema to be used for the partition result writer
     */
    private void partitionFile(long queryId, List<InputInfo> scanInputs,
                             String[] cols, TableScanFilter filter, int[] keyColumnIds,
                             List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult,
                             AtomicReference<TypeDescription> writerSchema)
    {
        for (InputInfo inputInfo : scanInputs)
        {
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

                PixelsReaderOption option = getReaderOption(queryId, cols, inputInfo);
                PixelsRecordReader recordReader = pixelsReader.read(option);
                TypeDescription rowBatchSchema = recordReader.getResultSchema();
                VectorizedRowBatch rowBatch;

                Partitioner partitioner = new Partitioner(partitionResult.size(), rowBatchSize,
                        rowBatchSchema, keyColumnIds);

                if (writerSchema.get() == null)
                {
                    writerSchema.weakCompareAndSet(null, rowBatchSchema);
                }
                Bitmap filtered = new Bitmap(rowBatchSize, true);
                Bitmap tmp = new Bitmap(rowBatchSize, false);
                do
                {
                    rowBatch = recordReader.readBatch(rowBatchSize);
                    filter.doFilter(rowBatch, filtered, tmp);
                    rowBatch.applyFilter(filtered);
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
                VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
                for (int hash = 0; hash < tailBatches.length; ++hash)
                {
                    if (!tailBatches[hash].isEmpty())
                    {
                        partitionResult.get(hash).add(tailBatches[hash]);
                    }
                }
            } catch (Exception e)
            {
                logger.error("failed to scan the file '" +
                        inputInfo.getPath() + "' and output the partitioning result", e);
            }
        }
    }
}
