package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.worker.common.WorkerCommon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


//interface StorageForStream {
//    enum Scheme {
//        http;
//    }
//}

public class StreamWorkerCommon extends WorkerCommon {
    private static final Logger logger = LogManager.getLogger(StreamWorkerCommon.class);
//    private static final ConfigFactory configFactory = ConfigFactory.Instance();
    private static Storage http;

    // This is a very temporary solution only for demo: We use one port in place of one file for simplicity.
    // In the future, one worker should only use one port in total and use URL or HTTP header value to distinguish different "files",
    //  because we won't have enough ports for all files.
    private static final ConcurrentHashMap<String, Integer> pathToPort = new ConcurrentHashMap<>();
    private static final AtomicInteger nextPort = new AtomicInteger(50500);

    public static int getPort(String path) {
        return pathToPort.computeIfAbsent(path, k -> nextPort.getAndIncrement());
    }
    public static void setPort(String path, int port) {
        pathToPort.putIfAbsent(path, port);
    }
//    private static Storage s3;
//    private static Storage minio;
//    private static Storage redis;
//    public static final int rowBatchSize;
//    private static final int pixelStride;
//    private static final int rowGroupSize;
//
//    static
//    {
//        rowBatchSize = Integer.parseInt(configFactory.getProperty("row.batch.size"));
//        pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
//        rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size"));
//    }

//    public static Storage getStorage(Storage.Scheme scheme)
//    {
//        if (requireNonNull(scheme) == Storage.Scheme.mock) {
//            return http;
//        }
//        throw new UnsupportedOperationException("scheme " + scheme + " is not supported in streaming mode");
//    }

    // hardcoded endpoints under test scenario before a real cluster is set up
    public static String getChildTestingEndpoint() { return "http://localhost:50499/"; }
    public static String getParentTestingEndpoint() { return "http://localhost:50499/"; }
    public static void initStorage(StorageInfo storageInfo, Boolean isOutput) throws IOException {
        if (storageInfo.getScheme() == Storage.Scheme.mock) {
            // streaming mode, return nothing
            if (isOutput)
            {
                // This is an output storage using HTTP. The opposite side must be waiting for a schema.
                // will need to send the schema one-off

            }
            return;
        }
        WorkerCommon.initStorage(storageInfo);
        logger.debug("Initialized minio storage: {}", minio);
    }

    public static void passSchemaToNextLevel(TypeDescription schema, StorageInfo storageInfo)
            throws IOException {
        if (storageInfo.getScheme() != Storage.Scheme.mock ||
                !Objects.equals(storageInfo.getRegion(), "http")) {
            return;  // Do nothing, not streaming mode
        }
//        logger.debug("passSchemaToNextLevel streaming mode");
        // Start a special port to pass schema
        StreamWorkerCommon.setPort(StreamWorkerCommon.getParentTestingEndpoint(), 50499);
        PixelsWriter pixelsWriter = getWriter(schema, null, getParentTestingEndpoint(), false, false, null);
        ((PixelsWriterStreamImpl)pixelsWriter).writeRowGroup();
        pixelsWriter.close();
        // outputStream.writeUTF(schema.toString());
    }

    public static Storage getStorage(Storage.Scheme scheme)
    {
        if (scheme == Storage.Scheme.mock) {
            // streaming mode, return nothing
            return null;
        }
        return WorkerCommon.getStorage(scheme);
    }

    public static TypeDescription getSchemaFromSplits(Storage storage, List<InputSplit> inputSplits)
            throws Exception {
        // It is assumed that one operator has its input splits of the same scheme. So we just need to
        //  check whether the first input split is from a file or from a stream.
        if (storage == null) {
            // Currently, the first packet from the stream brings the schema

            // Start a special port to receive schema
            PixelsReader pixelsReader = new PixelsReaderStreamImpl(StreamWorkerCommon.getChildTestingEndpoint());
            return pixelsReader.getFileSchema();
        }
        return WorkerCommon.getFileSchemaFromSplits(storage, inputSplits);

//        // To remain compatible with other existing code, we use the storage scheme mock and the region HTTP
//        //  to represent a streaming endpoint.
//        // 但inputInfo都是运行前预先决定好的，所以不能直接传送endpoint信息。
//        if (endpointStorageInfo.getScheme() != Storage.Scheme.mock ||
//                !Objects.equals(endpointStorageInfo.getRegion(), "HTTP")) {
//            throw new IllegalArgumentException("Storage is not HTTP under streaming mode");
//        }
//        // Run the HTTP server. Just wait for an arbitrary split to write the schema member. And then we are good.
    }

    public static TypeDescription getSchemaFromPaths(Storage storage, List<String> paths)
            throws Exception {
        // Same assumption as in getFileSchemaFromSplits
        if (storage == null)
        {
            PixelsReader pixelsReader = new PixelsReaderStreamImpl(StreamWorkerCommon.getChildTestingEndpoint());
            return pixelsReader.getFileSchema();
        }
        return WorkerCommon.getFileSchemaFromPaths(storage, paths);
    }

//    public static void getFileSchemaFromPaths(ExecutorService executor,
//                                              Storage leftStorage, Storage rightStorage,
//                                              AtomicReference<TypeDescription> leftSchema,
//                                              AtomicReference<TypeDescription> rightSchema,
//                                              List<String> leftPaths, List<String> rightPaths)
//    {
//        requireNonNull(executor, "executor is null");
//        requireNonNull(leftSchema, "leftSchema is null");
//        requireNonNull(rightSchema, "rightSchema is null");
//        requireNonNull(leftPaths, "leftPaths is null");
//        requireNonNull(rightPaths, "rightPaths is null");
//        // It is assumed that one operator has its input splits of the same scheme. So we just need to
//        //  check whether the first input split is from a file or from a stream.
//        if (leftStorage == null) {
//            // Currently, the first packet from the stream brings the schema
//
//            // Start a special port to receive schema
//            StreamWorkerCommon.setPort(StreamWorkerCommon.getChildTestingEndpoint(), 50499);
//            executor.submit(() -> {
//                try {
//                    PixelsReader pixelsReader = new PixelsReaderStreamImpl(StreamWorkerCommon.getChildTestingEndpoint());
//                    leftSchema.set(pixelsReader.getFileSchema());
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            });
//        }
//        if (rightStorage == null) {
//            // Currently, the first packet from the stream brings the schema
//
//            // Start a special port to receive schema
//            StreamWorkerCommon.setPort(StreamWorkerCommon.getChildTestingEndpoint(), 50499);
//            executor.submit(() -> {
//                try {
//                    PixelsReader pixelsReader = new PixelsReaderStreamImpl(StreamWorkerCommon.getChildTestingEndpoint());
//                    rightSchema.set(pixelsReader.getFileSchema());
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            });
//        }
//        WorkerCommon.getFileSchemaFromPaths(executor, leftStorage, rightStorage, leftSchema, rightSchema, leftPaths, rightPaths);
//    }

//    public static TypeDescription getResultSchema(TypeDescription fileSchema, String[] includeCols)
//    {
//        return WorkerCommon.getResultSchema(fileSchema, includeCols);
//    }

    public static PixelsReader getReader(Storage.Scheme storageScheme, String path) throws Exception
    {
        requireNonNull(storageScheme, "storageInfo is null");
        requireNonNull(path, "fileName is null");
        if (storageScheme == Storage.Scheme.mock) {
            logger.debug("getReader streaming mode, path: " + path + ", port: " + getPort(path));
            return new PixelsReaderStreamImpl("http://localhost:" + getPort(path) + "/");
        }
        else return WorkerCommon.getReader(path, WorkerCommon.getStorage(storageScheme));
    }

    public static PixelsWriter getWriter(TypeDescription schema, Storage storage,
                                         String outputPath, boolean encoding,  // String endpoint
                                         boolean isPartitioned, List<Integer> keyColumnIds)
    {
        // root operator是可以把结果直接传回VM的，但没时间写了，本次demo时只能把结果写到minio中。所以仍然是
        //  分类讨论，可能返回PixelsWriterStreamImpl或者PixelsWriterImpl
        if (storage == null || storage.getScheme() == Storage.Scheme.mock)  // streaming
        {
            logger.debug("getWriter streaming mode, path: " + outputPath + ", port: " + getPort(outputPath));
            // endpoint是HTTP的地址，outputPath相当于分区的hashValue。
            // 但在目前demo中，简单起见允许用一个outputPath占一个端口。
            requireNonNull(schema, "schema is null");
            requireNonNull(outputPath, "fileName is null");
            checkArgument(!isPartitioned || keyColumnIds != null,
                    "keyColumnIds is null whereas isPartitioned is true");

            PixelsWriterStreamImpl.Builder builder = PixelsWriterStreamImpl.newBuilder()
                    .setSchema(schema)
                    .setPixelStride(pixelStride)
                    .setRowGroupSize(rowGroupSize)
                    .setEndpoint("http://localhost:" + getPort(outputPath) + "/")  // (getParentTestingEndpoint())
                    // .setOverwrite(true) // set overwrite to true to avoid existence checking.
                    .setEncodingLevel(EncodingLevel.EL2) // it is worth to do encoding
                    .setPartitioned(isPartitioned);
            if (isPartitioned)
            {
                builder.setPartKeyColumnIds(keyColumnIds);
            }
            return builder.build();
        }
        else return WorkerCommon.getWriter(schema, storage, outputPath, encoding, isPartitioned, keyColumnIds);
    }

//    public static PixelsReaderOption getReaderOption(long transId, String[] cols, InputInfo input)
//    {
//        return WorkerCommon.getReaderOption(transId, cols, input);
//    }
}
