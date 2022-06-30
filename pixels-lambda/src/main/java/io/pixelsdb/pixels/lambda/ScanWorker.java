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
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.lambda.domain.InputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.InputSplit;
import io.pixelsdb.pixels.executor.lambda.input.ScanInput;
import io.pixelsdb.pixels.executor.lambda.output.ScanOutput;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;
import static io.pixelsdb.pixels.lambda.WorkerCommon.*;

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

    @Override
    public ScanOutput handleRequest(ScanInput event, Context context)
    {
        ScanOutput scanOutput = new ScanOutput();
        long startTime = System.currentTimeMillis();
        scanOutput.setStartTimeMs(startTime);
        scanOutput.setRequestId(context.getAwsRequestId());
        scanOutput.setSuccessful(true);
        scanOutput.setErrorMessage("");

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            String requestId = context.getAwsRequestId();

            long queryId = event.getQueryId();
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            checkArgument(event.getOutput().getScheme() == Storage.Scheme.minio,
                    "the storage scheme is not minio");
            checkArgument(event.getOutput().isRandomFileName(),
                    "random output file name is not enabled by the caller");
            String outputFolder = event.getOutput().getPath();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = event.getOutput().isEncoding();
            try
            {
                if (minio == null)
                {
                    ConfigMinIO(event.getOutput().getEndpoint(),
                            event.getOutput().getAccessKey(), event.getOutput().getSecretKey());
                    minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                }
            } catch (Exception e)
            {
                throw new PixelsWorkerException("failed to initialize MinIO storage", e);
            }
            String[] cols = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);
            int i = 0;
            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();
                String out = outputFolder + requestId + "_scan_" + i++;

                threadPool.execute(() -> {
                    try
                    {
                        int rowGroupNum = scanFile(queryId, scanInputs, cols, filter, out, encoding);
                        if (rowGroupNum > 0)
                        {
                            scanOutput.addOutput(out, rowGroupNum);
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

            scanOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
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
     * @param cols the included columns
     * @param filter the filter for the scan
     * @param outputPath fileName on s3 to store the scan results
     * @param encoding whether encode the scan results or not
     * @return the number of row groups that have been written into the output.
     */
    private int scanFile(long queryId, List<InputInfo> scanInputs, String[] cols,
                           TableScanFilter filter, String outputPath, boolean encoding)
    {
        PixelsWriter pixelsWriter = null;
        for (int i = 0; i < scanInputs.size(); ++i)
        {
            InputInfo inputInfo = scanInputs.get(i);
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

                if (pixelsWriter == null)
                {
                    pixelsWriter = getWriter(rowBatchSchema, minio, outputPath,
                            encoding, false, null);
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
                        pixelsWriter.addRowBatch(rowBatch);
                    }
                } while (!rowBatch.endOfFile);
            } catch (Exception e)
            {
                throw new PixelsWorkerException("failed to scan the file '" +
                        inputInfo.getPath() + "' and output the result", e);
            }
        }
        // Finished scanning all the files in the split.
        try
        {
            if (pixelsWriter != null)
            {
                pixelsWriter.close();
                while (!minio.exists(outputPath))
                {
                    // Wait for 10ms and see if the output file is visible.
                    TimeUnit.MILLISECONDS.sleep(10);
                }
                return pixelsWriter.getRowGroupNum();
            }
            return 0;
        } catch (Exception e)
        {
            throw new PixelsWorkerException(
                    "failed finish writing and close the output file '" + outputPath + "'", e);
        }
    }
}
