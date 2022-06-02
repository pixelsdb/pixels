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
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.executor.lambda.domain.InputInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

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

    /**
     * Read the schemas of the two joined tables, concurrently using the executor, thus
     * to reduce the latency of the schema reading.
     *
     * @param executor the executor, a.k.a., the thread pool.
     * @param storage the storage instance
     * @param leftSchema the atomic reference to return the schema of the left table
     * @param rightSchema the atomic reference to return the schema of the right table
     * @param leftPath the path of an input file of the left table
     * @param rightPath the path of an input file of the right table
     */
    public static void getFileSchema(ExecutorService executor, Storage storage,
                                     AtomicReference<TypeDescription> leftSchema,
                                     AtomicReference<TypeDescription> rightSchema,
                                     String leftPath, String rightPath)
    {
        requireNonNull(executor, "executor is null");
        requireNonNull(storage, "storage is null");
        requireNonNull(leftSchema, "leftSchema is null");
        requireNonNull(rightSchema, "rightSchema is null");
        requireNonNull(leftPath, "leftPath is null");
        requireNonNull(rightPath, "rightSchema is null");
        Future<?> leftFuture = executor.submit(() -> {
            try
            {
                PixelsReader reader = getReader(leftPath, storage);
                leftSchema.set(reader.getFileSchema());
                reader.close();
            } catch (IOException e)
            {
                logger.error("failed to read the schema of the left table");
            }
        });
        Future<?> rightFuture = executor.submit(() -> {
            try
            {
                PixelsReader reader = getReader(rightPath, storage);
                rightSchema.set(reader.getFileSchema());
                reader.close();
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

    /**
     * Get the schema that only includes the type descriptions of the included columns.
     * The returned schema can be used for the table scan result.
     *
     * @param fileSchema the schema of the file
     * @param includeCols the name of the included columns
     * @return the result schema
     */
    public static TypeDescription getResultSchema(TypeDescription fileSchema, String[] includeCols)
    {
        requireNonNull(fileSchema, "fileSchema is null");
        requireNonNull(includeCols, "includeCols is null");
        checkArgument(fileSchema.getCategory() == TypeDescription.Category.STRUCT,
                "fileSchema is not a STRUCT");
        checkArgument(fileSchema.getFieldNames().size() >= includeCols.length,
                "fileSchema does not contain enough fields");
        TypeDescription resultSchema = new TypeDescription(TypeDescription.Category.STRUCT);
        List<String> fileColumnNames = fileSchema.getFieldNames();
        List<TypeDescription> fileColumnTypes = fileSchema.getChildren();
        Map<String, Integer> allColumns = new HashMap<>(fileColumnNames.size());
        for (int i = 0; i < fileColumnNames.size(); ++i)
        {
            /**
             * According to SQL-92, column names (identifiers) are case-insensitive.
             * However, in many databases, including Pixels, column names are case-sensitive.
             */
            allColumns.put(fileColumnNames.get(i), i);
        }
        for (String columnName : includeCols)
        {
            if (allColumns.containsKey(columnName))
            {
                int i = allColumns.get(columnName);
                resultSchema.addField(columnName, fileColumnTypes.get(i));
            }
        }
        return resultSchema;
    }

    /**
     * Get a Pixels reader.
     *
     * @param filePath the file path
     * @param storage the storage instance
     * @return the Pixels reader
     * @throws IOException if failed to build the reader
     */
    public static PixelsReader getReader(String filePath, Storage storage) throws IOException
    {
        requireNonNull(filePath, "fileName is null");
        requireNonNull(storage, "storage is null");
        PixelsReaderImpl.Builder builder = PixelsReaderImpl.newBuilder()
                .setStorage(storage)
                .setPath(filePath)
                .setEnableCache(false)
                .setCacheOrder(new ArrayList<>())
                .setPixelsCacheReader(null)
                .setPixelsFooterCache(footerCache);
        PixelsReader pixelsReader = builder.build();
        return pixelsReader;
    }

    /**
     * Get a Pixels writer.
     *
     * @param schema the schema of the file to write
     * @param storage the storage instance
     * @param filePath the file path
     * @param encoding whether this file is encoded or not
     * @param isPartitioned whether this file is partitioned or not
     * @param keyColumnIds the ids of the key columns if this file is partitioned.
     *                     It can be null if partitioned is false.
     * @return the Pixels writer
     */
    public static PixelsWriter getWriter(TypeDescription schema, Storage storage,
                                         String filePath, boolean encoding,
                                         boolean isPartitioned, List<Integer> keyColumnIds)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(filePath, "fileName is null");
        requireNonNull(storage, "storage is null");
        checkArgument(!isPartitioned || keyColumnIds != null,
                "keyColumnIds is null whereas isPartitioned is true");
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

    /**
     * Create the reader option for a record reader of the given input file.
     *
     * @param queryId the query id
     * @param cols the column names in the partitioned file
     * @param input the information of the input file
     * @return the reader option
     */
    public static PixelsReaderOption getReaderOption(long queryId, String[] cols, InputInfo input)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.queryId(queryId);
        option.includeCols(cols);
        option.rgRange(input.getRgStart(), input.getRgLength());
        return option;
    }
}
