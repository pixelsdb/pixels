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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.predicate.TableScanFilter;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The response is a list of files read and then written to s3.
 *
 * @author tiannan
 * @author hank
 * Created in: 03.2022
 */
public class ScanWorker implements RequestHandler<Map<String, ArrayList<String>>, String>
{
    private static final Logger logger = LoggerFactory.getLogger(ScanWorker.class);
    private static final PixelsFooterCache footerCache = new PixelsFooterCache();
    private static final int pixelStride;
    private static final int rowGroupSize;
    // blockSize and replication have no effects for S3, therefore we set the default value here.
    private static final long blockSize = 2048L * 1024L * 1024L;
    private static final short replication = (short) 1;
    private static Storage storage;

    static
    {
        pixelStride = Integer.parseInt(ConfigFactory.Instance().getProperty("pixel.stride"));
        rowGroupSize = Integer.parseInt(ConfigFactory.Instance().getProperty("row.group.size"));
        try
        {
            storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);

        } catch (IOException e)
        {
            logger.error("failed to initialize s3 storage.", e);
        }
    }

    @Override
    public String handleRequest(Map<String, ArrayList<String>> event, Context context)
    {
        try
        {
            ExecutorService threadPool = Executors.newFixedThreadPool(12);
            String requestId = context.getAwsRequestId();

            ArrayList<String> fileNames = event.get("inputs");
            String outputDir = event.get("outputs").get(0); // currently, we only use one output directory.
            if (!outputDir.endsWith("/"))
            {
                outputDir += "/";
            }
            String[] cols = event.get("cols").toArray(new String[0]);
            // currently, we use an integrated table scan filter on each table.
            TableScanFilter filter = JSON.parseObject(event.get("filter").get(0), TableScanFilter.class);
            StringBuilder response = new StringBuilder();
            for (int i = 0; i < fileNames.size(); i++)
            {
                String in = fileNames.get(i);
                String out = outputDir + requestId + "_out_" + i;

                if (i > 0)
                {
                    response.append(",");
                }
                response.append(out);

                threadPool.submit(() -> scanFile(in, 102400, cols, filter, out));
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                logger.error("interrupted while waiting for the termination of scan.", e);
            }
            // return the output file names in csv format.
            return response.toString();
        } catch (Exception e)
        {
            logger.error("error during scan.", e);
            return null;
        }
    }

    /**
     * @param inputPath
     * @param batchSize
     * @param cols
     * @param outputPath fileName on s3 to store pixels readers' results
     * @return
     */
    public String scanFile(String inputPath, int batchSize, String[] cols, TableScanFilter filter, String outputPath)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);
        VectorizedRowBatch rowBatch;

        try (PixelsReader pixelsReader = getReader(inputPath);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
            TypeDescription rowBatchSchema = recordReader.getResultSchema();

            PixelsWriter pixelsWriter = getWriter(rowBatchSchema, outputPath);
            Bitmap filtered = new Bitmap(batchSize, true);
            Bitmap tmp = new Bitmap(batchSize, false);
            while (true)
            {
                rowBatch = recordReader.readBatch(batchSize);
                filter.doFilter(rowBatch, filtered, tmp);
                rowBatch.applyFilter(filtered);
                if (rowBatch.size > 0)
                {
                    pixelsWriter.addRowBatch(rowBatch);
                }
                if (rowBatch.endOfFile)
                {
                    pixelsWriter.close();
                    break;
                }
            }
        } catch (Exception e)
        {
            logger.error("failed to scan the file '" + inputPath + "' and output the result.", e);
        }
        return "success";
    }

    private PixelsReader getReader(String fileName)
    {
        PixelsReader pixelsReader = null;
        try
        {
            PixelsReaderImpl.Builder builder = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(fileName)
                    .setEnableCache(false)
                    .setCacheOrder(new ArrayList<>())
                    .setPixelsCacheReader(null)
                    .setPixelsFooterCache(footerCache);
            pixelsReader = builder.build();

        } catch (Exception e)
        {
            logger.error("failed to create pixels reader.", e);
        }
        return pixelsReader;
    }

    private PixelsWriter getWriter(TypeDescription schema, String filePath)
    {
        PixelsWriter pixelsWriter =
                PixelsWriterImpl.newBuilder()
                        .setSchema(schema)
                        .setPixelStride(pixelStride)
                        .setRowGroupSize(rowGroupSize)
                        .setStorage(storage)
                        .setFilePath(filePath)
                        .setBlockSize(blockSize)
                        .setReplication(replication)
                        .setBlockPadding(true)
                        .setOverwrite(true) // set overwrite to true to avoid existence checking.
                        .setEncoding(true) // it is worth to do encoding
                        .setCompressionBlockSize(1)
                        .build();
        return pixelsWriter;
    }
}
