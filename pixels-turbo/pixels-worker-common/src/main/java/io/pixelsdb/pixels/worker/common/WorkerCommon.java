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
package io.pixelsdb.pixels.worker.common;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.storage.redis.Redis.ConfigRedis;
import static io.pixelsdb.pixels.storage.s3.Minio.ConfigMinio;
import static java.util.Objects.requireNonNull;

/**
 * Some common functions for the lambda workers.
 * @author hank
 * @create 2022-05-15
 */
public class WorkerCommon
{
    private static final Logger logger = LogManager.getLogger(WorkerCommon.class);
    private static final PixelsFooterCache footerCache = new PixelsFooterCache();
    private static final ConfigFactory configFactory = ConfigFactory.Instance();
    private static Storage s3;
    protected static Storage minio;
    private static Storage redis;
    private static Storage stream;
    public static final int rowBatchSize;
    protected static final int pixelStride;
    protected static final int rowGroupSize;
    protected static int port;
    protected static String coordinatorIp;
    protected static int coordinatorPort;
    private static final ConcurrentHashMap<String, PixelsReader> streamReaders;

    static
    {
        rowBatchSize = Integer.parseInt(configFactory.getProperty("row.batch.size"));
        pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size"));
        port = Integer.parseInt(configFactory.getProperty("executor.worker.exchange.port"));
        coordinatorIp = configFactory.getProperty("worker.coordinate.server.host");
        coordinatorPort = Integer.parseInt(configFactory.getProperty("worker.coordinate.server.port"));
        streamReaders = new ConcurrentHashMap<>();
    }

    public static void initStorage(StorageInfo storageInfo)
    {
        try
        {
            if (WorkerCommon.s3 == null && storageInfo.getScheme() == Storage.Scheme.s3)
            {
                WorkerCommon.s3 = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
            }
            else if (WorkerCommon.minio == null && storageInfo.getScheme() == Storage.Scheme.minio)
            {
                ConfigMinio(storageInfo.getRegion(), storageInfo.getEndpoint(),
                        storageInfo.getAccessKey(), storageInfo.getSecretKey());
                WorkerCommon.minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
            }
            else if (WorkerCommon.redis == null && storageInfo.getScheme() == Storage.Scheme.redis)
            {
                ConfigRedis(storageInfo.getEndpoint(), storageInfo.getAccessKey(), storageInfo.getSecretKey());
                WorkerCommon.redis = StorageFactory.Instance().getStorage(Storage.Scheme.redis);
            }
            else if (WorkerCommon.stream == null && storageInfo.getScheme() == Storage.Scheme.httpstream)
            {
                WorkerCommon.stream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
            }
        } catch (Throwable e)
        {
            throw new WorkerException("failed to initialize the storage of scheme " + storageInfo.getScheme(), e);
        }
    }

    public static Storage getStorage(Storage.Scheme scheme)
    {
        switch (scheme)
        {
            case s3:
                return s3;
            case minio:
                return minio;
            case redis:
                return redis;
            case httpstream:
                return stream;
        }
        throw new UnsupportedOperationException("scheme " + scheme + " is not supported");
    }

    /**
     * Read the schemas of the two joined tables, concurrently using the executor, thus
     * to reduce the latency of the schema reading.
     *
     * @param executor the executor, a.k.a., the thread pool.
     * @param leftStorage the storage instance of the left table
     * @param rightStorage the storage instance of the right table
     * @param leftSchema the atomic reference to return the schema of the left table
     * @param rightSchema the atomic reference to return the schema of the right table
     * @param leftInputSplits the input splits of the left table
     * @param rightInputSplits the input splits of the right table
     */
    public static void getFileSchemaFromSplits(ExecutorService executor,
                                               Storage leftStorage, Storage rightStorage,
                                               AtomicReference<TypeDescription> leftSchema,
                                               AtomicReference<TypeDescription> rightSchema,
                                               List<InputSplit> leftInputSplits, List<InputSplit> rightInputSplits)
    {
        requireNonNull(executor, "executor is null");
        requireNonNull(leftStorage, "leftStorage is null");
        requireNonNull(rightStorage, "rightStorage is null");
        requireNonNull(leftSchema, "leftSchema is null");
        requireNonNull(rightSchema, "rightSchema is null");
        requireNonNull(leftInputSplits, "leftInputSplits is null");
        requireNonNull(rightInputSplits, "rightInputSplits is null");
        Future<?> leftFuture = executor.submit(() -> {
            try
            {
                leftSchema.set(getFileSchemaFromSplits(leftStorage, leftInputSplits));
            } catch (IOException | InterruptedException e)
            {
                logger.error("failed to read the file schema for the left table", e);
            }
        });
        Future<?> rightFuture = executor.submit(() -> {
            try
            {
                rightSchema.set(getFileSchemaFromSplits(rightStorage, rightInputSplits));
            } catch (IOException | InterruptedException e)
            {
                logger.error("failed to read the file schema for the right table", e);
            }
        });
        try
        {
            leftFuture.get();
            rightFuture.get();
        } catch (Throwable e)
        {
            logger.error("interrupted while waiting for the termination of schema read", e);
        }
    }

    /**
     * Read the schemas of the two joined tables, concurrently using the executor, thus
     * to reduce the latency of the schema reading.
     *
     * @param executor the executor, a.k.a., the thread pool
     * @param leftStorage the storage instance of the left table
     * @param rightStorage the storage instance of the right table
     * @param leftSchema the atomic reference to return the schema of the left table
     * @param rightSchema the atomic reference to return the schema of the right table
     * @param leftPaths the paths of the input files of the left table
     * @param rightPaths the paths of the input files of the right table
     */
    public static void getFileSchemaFromPaths(ExecutorService executor,
                                              Storage leftStorage, Storage rightStorage,
                                              AtomicReference<TypeDescription> leftSchema,
                                              AtomicReference<TypeDescription> rightSchema,
                                              List<String> leftPaths, List<String> rightPaths)
    {
        requireNonNull(executor, "executor is null");
        requireNonNull(leftStorage, "leftStorage is null");
        requireNonNull(rightStorage, "rightStorage is null");
        requireNonNull(leftSchema, "leftSchema is null");
        requireNonNull(rightSchema, "rightSchema is null");
        requireNonNull(leftPaths, "leftPaths is null");
        requireNonNull(rightPaths, "rightPaths is null");
        List<Future<?>> leftFutures = new ArrayList<>();
        if (leftStorage.getScheme().equals(Storage.Scheme.httpstream))
        {
            for (String path: leftPaths)
            {
                leftFutures.add(executor.submit(() -> {
                    try
                    {
                        leftSchema.set(getFileSchemaFromPath(leftStorage, path));
                    } catch (IOException e)
                    {
                        logger.error("failed to read the file schema for the left table", e);
                    }
                }));
            }
        } else
        {
            leftFutures.add(executor.submit(() -> {
                try
                {
                    leftSchema.set(getFileSchemaFromPaths(leftStorage, leftPaths));
                } catch (IOException | InterruptedException e)
                {
                    logger.error("failed to read the file schema for the left table", e);
                }
            }));
        }
        List<Future<?>> rightFutures = new ArrayList<>();
        if (rightStorage.getScheme().equals(Storage.Scheme.httpstream))
        {
            for (String path: rightPaths)
            {
                rightFutures.add(executor.submit(() -> {
                    try
                    {
                        rightSchema.set(getFileSchemaFromPath(rightStorage, path));
                    } catch (IOException e)
                    {
                        logger.error("failed to read the file schema for the right table", e);
                    }
                }));
            }
        } else
        {
            rightFutures.add(executor.submit(() -> {
                try
                {
                    rightSchema.set(getFileSchemaFromPaths(rightStorage, rightPaths));
                } catch (IOException | InterruptedException e)
                {
                    logger.error("failed to read the file schema for the right table", e);
                }
            }));
        }

        try
        {
            for (Future<?> future: leftFutures)
            {
                future.get();
            }
            for (Future<?> future: rightFutures)
            {
                future.get();
            }
        } catch (Throwable e)
        {
            logger.error("interrupted while waiting for the termination of schema read", e);
        }
    }

    /**
     * Read the schemas of the table.
     *
     * @param storage the storage instance
     * @param inputSplits the list of input info of the input files of the table
     * @return the file schema of the table
     */
    public static TypeDescription getFileSchemaFromSplits(Storage storage, List<InputSplit> inputSplits)
            throws IOException, InterruptedException
    {
        requireNonNull(storage, "storage is null");
        requireNonNull(inputSplits, "inputSplits is null");
        TypeDescription fileSchema = null;
        while (true)
        {
            for (InputSplit inputSplit : inputSplits)
            {
                String checkedPath = null;
                for (InputInfo inputInfo : inputSplit.getInputInfos())
                {
                    if (checkedPath != null && checkedPath.equals(inputInfo.getPath()))
                    {
                        continue;
                    }
                    try
                    {
                        checkedPath = inputInfo.getPath();
                        PixelsReader reader = getReader(checkedPath, storage);
                        fileSchema = reader.getFileSchema();
                        if (!storage.getScheme().equals(Storage.Scheme.httpstream))
                        {
                            reader.close();
                            return fileSchema;
                        }
                    } catch (Throwable e)
                    {
                        if (e instanceof IOException)
                        {
                            continue;
                        }
                        throw new IOException("failed to read file schema", e);
                    }
                }
            }
            if (storage.getScheme().equals(Storage.Scheme.httpstream))
            {
                return fileSchema;
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }
    }
    
    /**
     * Read the schemas of the table.
     *
     * @param storage the storage instance
     * @param paths the list of paths of the input files of the table
     * @return the file schema of the table
     */
    public static TypeDescription getFileSchemaFromPaths(Storage storage, List<String> paths)
            throws IOException, InterruptedException
    {
        requireNonNull(storage, "storage is null");
        requireNonNull(paths, "paths is null");

        TypeDescription fileSchema = null;
        while (true)
        {
            for (String path : paths)
            {
                try
                {
                    PixelsReader reader = getReader(path, storage);
                    fileSchema = reader.getFileSchema();
                    if (!storage.getScheme().equals(Storage.Scheme.httpstream))
                    {
                        reader.close();
                        return fileSchema;
                    }
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        logger.error("failed to read file schema", e);
                        continue;
                    }
                    throw new IOException("failed to read file schema", e);
                }
            }
            if (storage.getScheme().equals(Storage.Scheme.httpstream))
            {
                return fileSchema;
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }
    }

    public static TypeDescription getFileSchemaFromPath(Storage storage, String path)
            throws IOException
    {
        requireNonNull(storage, "storage is null");
        requireNonNull(path, "path is null");

        TypeDescription fileSchema = null;
        try
        {
            logger.info("try to get path " + path);
            PixelsReader reader = getReader(path, storage);
            fileSchema = reader.getFileSchema();
            logger.info("succeed to get path " + path);
        } catch (Throwable e)
        {
            if (e instanceof IOException)
            {
                logger.error("failed to read file schema", e);
            }
            throw new IOException("failed to read file schema", e);
        }
        return fileSchema;
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
        PixelsReader pixelsReader;
        if (storage.getScheme().equals(Storage.Scheme.httpstream))
        {
            if (!streamReaders.containsKey(filePath) || streamReaders.get(filePath).getFileSchema() == null)
            {
                pixelsReader = PixelsReaderStreamImpl.newBuilder()
                        .setStorage(storage)
                        .setPath(filePath)
                        .build();
                streamReaders.put(filePath, pixelsReader);
            }
            pixelsReader = streamReaders.get(filePath);
        } else
        {
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(filePath)
                    .setEnableCache(false)
                    .setCacheOrder(ImmutableList.of())
                    .setPixelsCacheReader(null)
                    .setPixelsFooterCache(footerCache)
                    .build();
        }
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
        PixelsWriter writer;
        if (storage.getScheme() == Storage.Scheme.httpstream)
        {
            PixelsWriterStreamImpl.Builder builder =  PixelsWriterStreamImpl.newBuilder()
                    .setSchema(schema)
                    .setPixelStride(pixelStride)
                    .setRowGroupSize(rowGroupSize)
                    .setStorage(storage)
                    .setPath(filePath)
                    .setEncodingLevel(EncodingLevel.EL2)
                    .setPartitioned(isPartitioned);
            if (isPartitioned)
            {
                builder.setPartKeyColumnIds(keyColumnIds);
            }
            writer = builder.build();
        } else
        {
            PixelsWriterImpl.Builder builder = PixelsWriterImpl.newBuilder()
                    .setSchema(schema)
                    .setPixelStride(pixelStride)
                    .setRowGroupSize(rowGroupSize)
                    .setStorage(storage)
                    .setPath(filePath)
                    .setOverwrite(true) // set overwrite to true to avoid existence checking.
                    .setEncodingLevel(EncodingLevel.EL2) // it is worth to do encoding
                    .setPartitioned(isPartitioned);
            if (isPartitioned)
            {
                builder.setPartKeyColumnIds(keyColumnIds);
            }
            writer = builder.build();
        }
        return writer;
    }

    /**
     * Create the reader option for a record reader of the given input file.
     *
     * @param transId the transaction id
     * @param timestamp the transaction timestamp
     * @param cols the column names in the partitioned file
     * @param input the information of the input file
     * @return the reader option
     */
    public static PixelsReaderOption getReaderOption(long transId, long timestamp, String[] cols, InputInfo input)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(transId);
        option.transTimestamp(timestamp);
        option.includeCols(cols);
        option.rgRange(input.getRgStart(), input.getRgLength());
        return option;
    }

    public static PixelsReaderOption getReaderOption(long transId, String[] cols)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(transId);
        option.includeCols(cols);
        option.rgRange(0, -1);
        return option;
    }

    /**
     * Create the reader option for a record reader of the given hash partition in a partitioned file.
     * It must be checked outside that the given hash partition info exists in the file.
     * @param transId the transaction id
     * @param timestamp the transaction timestamp
     * @param cols the column names in the partitioned file
     * @param pixelsReader the reader of the partitioned file
     * @param hashValue the hash value of the given hash partition
     * @param numPartition the total number of partitions
     * @return the reader option
     */
    public static PixelsReaderOption getReaderOption(long transId, long timestamp, String[] cols, PixelsReader pixelsReader,
                                               int hashValue, int numPartition)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.transId(transId);
        option.transTimestamp(timestamp);
        option.includeCols(cols);
        if (pixelsReader instanceof PixelsReaderImpl)
        {
            if (pixelsReader.getRowGroupNum() == numPartition)
            {
                option.rgRange(hashValue, 1);
            } else
            {
                for (int i = 0; i < pixelsReader.getRowGroupNum(); ++i)
                {
                    PixelsProto.RowGroupInformation info = pixelsReader.getRowGroupInfo(i);
                    if (info.getPartitionInfo().getHashValue() == hashValue)
                    {
                        // Note: DO NOT use hashValue as the row group start index.
                        option.rgRange(i, 1);
                        break;
                    }
                }
            }
        }
        return option;
    }

    public static void setPerfMetrics(Output output, WorkerMetrics collector)
    {
        output.setCumulativeInputCostMs((int) Math.round(collector.getInputCostNs() / 1000_000.0));
        output.setCumulativeComputeCostMs((int) Math.round(collector.getComputeCostNs() / 1000_000.0));
        output.setCumulativeOutputCostMs((int) Math.round(collector.getOutputCostNs() / 1000_000.0));
        output.setTotalReadBytes(collector.getReadBytes());
        output.setTotalWriteBytes(collector.getWriteBytes());
        output.setNumReadRequests(collector.getNumReadRequests());
        output.setNumWriteRequests(collector.getNumWriteRequests());
    }

    public static String getIpAddress()
    {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                if (netInterface.isLoopback() || netInterface.isVirtual() || !netInterface.isUp()) {
                    continue;
                } else {
                    Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        ip = addresses.nextElement();
                        if (ip != null && ip instanceof Inet4Address) {
                            return ip.getHostAddress();
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("failed to get ip" + e.toString());
        }
        return "";
    }

    public static int getPort()
    {
        return port;
    }

    public static String getCoordinatorIp()
    {
        return coordinatorIp;
    }

    public static int getCoordinatorPort()
    {
        return coordinatorPort;
    }
}
