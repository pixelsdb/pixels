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

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Some common functions for the lambda workers.
 * @author hank
 * @date 15/05/2022
 */
public class WorkerCommon
{
    private static final Logger logger = LoggerFactory.getLogger(WorkerCommon.class);
    private static final PixelsFooterCache footerCache = new PixelsFooterCache();
    private static final ConfigFactory configFactory = ConfigFactory.Instance();
    public static final int rowBatchSize;
    private static final int pixelStride;
    private static final int rowGroupSize;

    static
    {
        rowBatchSize = Integer.parseInt(configFactory.getProperty("row.batch.size"));
        pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size"));
    }

    public static void getSchema(ExecutorService executor, Storage storage,
                                 AtomicReference<TypeDescription> leftSchema,
                                 AtomicReference<TypeDescription> rightSchema,
                                 String leftPath, String rightPath)
    {
        Future<?> leftFuture = executor.submit(() -> {
            try
            {
                leftSchema.set(getReader(leftPath, storage).getFileSchema());
            } catch (IOException e)
            {
                logger.error("failed to read the schema of the left table");
            }
        });
        Future<?> rightFuture = executor.submit(() -> {
            try
            {
                rightSchema.set(getReader(rightPath, storage).getFileSchema());
            } catch (IOException e)
            {
                logger.error("failed to read the schema of the right table");
            }
        });
        try
        {
            leftFuture.get();
            rightFuture.get();
        } catch (Exception e)
        {
            logger.error("interrupted while waiting for the termination of schema read", e);
        }
    }

    public static PixelsReader getReader(String fileName, Storage storage) throws IOException
    {
        PixelsReaderImpl.Builder builder = PixelsReaderImpl.newBuilder()
                .setStorage(storage)
                .setPath(fileName)
                .setEnableCache(false)
                .setCacheOrder(new ArrayList<>())
                .setPixelsCacheReader(null)
                .setPixelsFooterCache(footerCache);
        PixelsReader pixelsReader = builder.build();
        return pixelsReader;
    }

    public static PixelsWriter getWriter(TypeDescription schema, Storage storage,
                                         String filePath, boolean encoding,
                                         boolean isPartitioned, List<Integer> keyColumnIds)
    {
        PixelsWriterImpl.Builder builder = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setStorage(storage)
                .setPath(filePath)
                .setOverwrite(true) // set overwrite to true to avoid existence checking.
                .setEncoding(encoding)
                .setPartitioned(isPartitioned);
        if (isPartitioned)
        {
            builder.setPartKeyColumnIds(keyColumnIds);
        }
        return builder.build();
    }
}
