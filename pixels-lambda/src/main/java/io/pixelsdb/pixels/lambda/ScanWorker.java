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
import io.pixelsdb.pixels.executor.lambda.ScanInput;
import io.pixelsdb.pixels.executor.lambda.ScanInput.InputInfo;
import io.pixelsdb.pixels.executor.lambda.ScanOutput;
import io.pixelsdb.pixels.core.predicate.TableScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    private static Storage s3;
    private static Storage minio;

    static
    {
        try
        {
            s3 = StorageFactory.Instance().getStorage(Storage.Scheme.s3);

        } catch (Exception e)
        {
            logger.error("failed to initialize AWS S3 storage", e);
        }
    }

    @Override
    public ScanOutput handleRequest(ScanInput event, Context context)
    {
        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            String requestId = context.getAwsRequestId();

            long queryId = event.getQueryId();
            ArrayList<InputInfo> inputs = event.getInputs();
            int splitSize = event.getSplitSize();
            String outputFolder = event.getOutput().getFolder();
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
                logger.error("failed to initialize MinIO storage", e);
            }
            String[] cols = event.getCols();
            TableScanFilter filter = JSON.parseObject(event.getFilter(), TableScanFilter.class);
            ScanOutput scanOutput = new ScanOutput();
            for (int i = 0; i < inputs.size();)
            {
                int numRg = 0;
                ArrayList<InputInfo> scanInputs = new ArrayList<>();
                while (numRg < splitSize)
                {
                    InputInfo info = inputs.get(i++);
                    scanInputs.add(info);
                    numRg += info.getRgLength();
                }
                String out = outputFolder + requestId + "_out_" + i;

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
                        logger.error("error during scan", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                logger.error("interrupted while waiting for the termination of scan", e);
            }

            return scanOutput;
        } catch (Exception e)
        {
            logger.error("error during scan", e);
            return null;
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
    private int scanFile(long queryId, ArrayList<InputInfo> scanInputs, String[] cols,
                           TableScanFilter filter, String outputPath, boolean encoding)
    {
        PixelsWriter pixelsWriter = null;
        for (int i = 0; i < scanInputs.size(); ++i)
        {
            InputInfo inputInfo = scanInputs.get(i);
            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.queryId(queryId);
            option.includeCols(cols);
            option.rgRange(inputInfo.getRgStart(), inputInfo.getRgLength());
            VectorizedRowBatch rowBatch;

            try (PixelsReader pixelsReader = getReader(inputInfo.getPath(), s3);
                 PixelsRecordReader recordReader = pixelsReader.read(option))
            {
                if (!recordReader.isValid())
                {
                    /*
                     * If the record reader is invalid, it is likely that the rgRange
                     * in the read option is out of bound (i.e., this is the last file
                     * in the table that does not have enough row groups to read).
                     */
                    break;
                }

                TypeDescription rowBatchSchema = recordReader.getResultSchema();

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
                logger.error("failed to scan the file '" +
                        inputInfo.getPath() + "' and output the result", e);
            }
        }
        // Finished scanning all the files in the split.
        try
        {
            pixelsWriter.close();
            while (true)
            {
                try
                {
                    if (minio.getStatus(outputPath) != null)
                    {
                        break;
                    }
                } catch (Exception e)
                {
                    // Wait for 10ms and see if the output file is visible.
                    TimeUnit.MILLISECONDS.sleep(10);
                }
            }
        } catch (Exception e)
        {
            logger.error("failed finish writing and close the output file '" + outputPath + "'", e);
        }
        return pixelsWriter.getRowGroupNum();
    }
}
