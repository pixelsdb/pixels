package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.worker.common.WorkerCommon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StreamWorkerCommon extends WorkerCommon {
    private static final Logger logger = LogManager.getLogger(StreamWorkerCommon.class);
//    private static final ConfigFactory configFactory = ConfigFactory.Instance();
    private static Storage http;

    private static final PixelsReaderStreamImpl.BlockingMap<String, Integer> pathToPort = new PixelsReaderStreamImpl.BlockingMap<>();
    private static final ConcurrentHashMap<String, Integer> pathToSchemaPort = new ConcurrentHashMap<>();
    private static final AtomicInteger nextPort = new AtomicInteger(50100);
    private static final AtomicInteger schemaPorts = new AtomicInteger(50099);

    public static int getPort(String path) {
        try {
            int ret = pathToPort.get(path);
            setPort(path, ret);  // ArrayBlockingQueue.take() removes the element from the queue, so we need to put it back
            return ret;
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            return -1;
        }
    }
    public static int getOrSetPort(String path) throws InterruptedException {
        if (pathToPort.exist(path)) return getPort(path);
        else {
            int port = nextPort.getAndIncrement();
            setPort(path, port);
            return port;
        }
    }
    public static void setPort(String path, int port) {
        pathToPort.put(path, port);
    }
//    public static void delPort(int port) {
//        logger.debug("delPort: " + port);
//        assert(pathToPort.entrySet().removeIf(entry -> entry.getValue() == port));
//    }

    public static int getSchemaPort(String path) { return pathToSchemaPort.computeIfAbsent(path, k -> schemaPorts.getAndDecrement()); }
    public static void initStorage(StorageInfo storageInfo, Boolean isOutput) throws IOException {
        if (storageInfo.getScheme() == Storage.Scheme.mock) {
            // streaming mode, return nothing
            if (isOutput)
            {
                // This is an output storage using HTTP. The opposite side must be waiting for a schema.
                //  will need to send the schema one-off.
                // But currently is done in BaseStreamWorkers

            }
            return;
        }
        WorkerCommon.initStorage(storageInfo);
        logger.debug("Initialized minio storage: {}", minio);
    }

    public static void passSchemaToNextLevel(TypeDescription schema, StorageInfo storageInfo, OutputInfo outputInfo)
            throws IOException {
        if (storageInfo.getScheme() != Storage.Scheme.mock ||
                !Objects.equals(storageInfo.getRegion(), "http")) {
            throw new IllegalArgumentException("Storage is not HTTP under streaming mode");
        }
        // Start a special port to pass schema
        passSchemaToNextLevel(schema, storageInfo, "http://localhost:" + getSchemaPort(outputInfo.getPath()) + "/");
    }

    public static void passSchemaToNextLevel(TypeDescription schema, StorageInfo storageInfo, String endpoint)
            throws IOException {
        if (storageInfo.getScheme() != Storage.Scheme.mock ||
                !Objects.equals(storageInfo.getRegion(), "http")) {
            throw new IllegalArgumentException("Storage is not HTTP under streaming mode");
        }
//        logger.debug("passSchemaToNextLevel streaming mode");
        PixelsWriter pixelsWriter = getWriter(schema, null, endpoint, false, false, -1, null, null, true);
        ((PixelsWriterStreamImpl)pixelsWriter).writeRowGroup();
        pixelsWriter.close();
        // outputStream.writeUTF(schema.toString());
    }

    public static void passSchemaToNextLevel(TypeDescription schema, StorageInfo storageInfo, List<String> endpoints)
            throws IOException {
        for (String endpoint : endpoints)
            passSchemaToNextLevel(schema, storageInfo, endpoint);  // todo: can do it in parallel
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
        // XXX: Consider just start the HTTP server and wait for an arbitrary split to write the schema member,
        //  instead of getting the schema in advance like we do now.
        if (storage == null) {
            PixelsReader pixelsReader = new PixelsReaderStreamImpl(StreamWorkerCommon.getSchemaPort(inputSplits.get(0).getInputInfos().get(0).getPath()));
            return pixelsReader.getFileSchema();
        }
        return WorkerCommon.getFileSchemaFromSplits(storage, inputSplits);
    }

    public static TypeDescription getSchemaFromPaths(Storage storage, List<String> paths)
            throws Exception {
        if (storage == null)
        {
            PixelsReader pixelsReader = new PixelsReaderStreamImpl(StreamWorkerCommon.getSchemaPort(paths.get(0)));
            return pixelsReader.getFileSchema();
        }
        return WorkerCommon.getFileSchemaFromPaths(storage, paths);
    }

    public static void getSchemaFromPaths(ExecutorService executor,
                                              Storage leftStorage, Storage rightStorage,
                                              AtomicReference<TypeDescription> leftSchema,
                                              AtomicReference<TypeDescription> rightSchema,
                                              List<String> leftPaths, List<String> rightPaths)
    {
        requireNonNull(executor, "executor is null");
        requireNonNull(leftSchema, "leftSchema is null");
        requireNonNull(rightSchema, "rightSchema is null");
        requireNonNull(leftPaths, "leftPaths is null");
        requireNonNull(rightPaths, "rightPaths is null");
        if (leftStorage == null && rightStorage == null) {
            // Currently, the first packet from the stream brings the schema
            Future<?> leftFuture = executor.submit(() -> {
                try {
                    PixelsReader pixelsReader = new PixelsReaderStreamImpl(StreamWorkerCommon.getSchemaPort(leftPaths.get(0)));  // XXX: pixelsReader.close()
                    leftSchema.set(pixelsReader.getFileSchema());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            Future<?> rightFuture = executor.submit(() -> {
                try {
                    PixelsReader pixelsReader = new PixelsReaderStreamImpl(StreamWorkerCommon.getSchemaPort(rightPaths.get(0)));  // XXX: pixelsReader.close()
                    rightSchema.set(pixelsReader.getFileSchema());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            try {
                leftFuture.get();
                rightFuture.get();
            } catch (Throwable e) {
                logger.error("interrupted while waiting for the termination of schema read", e);
            }
        }
        else WorkerCommon.getFileSchemaFromPaths(executor, leftStorage, rightStorage, leftSchema, rightSchema, leftPaths, rightPaths);
    }

    public static PixelsReader getReader(String filePath, Storage storage) throws IOException
    {
        throw new IOException("Forbidden to call WorkerCommon.getReader() from StringWorkerCommon");
    }

    public static PixelsReader getReader(Storage.Scheme storageScheme, String path) throws Exception
    {
        return getReader(storageScheme, path, false, -1);
    }

    // numHashes: the total number of hashes inside a partition
    public static PixelsReader getReader(Storage.Scheme storageScheme, String path, boolean partitioned, int numHashes) throws Exception
    {
        requireNonNull(storageScheme, "storageInfo is null");
        requireNonNull(path, "fileName is null");
        if (storageScheme == Storage.Scheme.mock) {
            logger.debug("getReader streaming mode, path: " + path + ", port: " + getOrSetPort(path));
            return new PixelsReaderStreamImpl("http://localhost:" + getOrSetPort(path) + "/", partitioned, numHashes);  // todo: let the getPort() go through only after the server started
        }
        else return WorkerCommon.getReader(path, WorkerCommon.getStorage(storageScheme));
    }

    public static PixelsWriter getWriter(TypeDescription schema, Storage storage,
                                         String outputPath, boolean encoding)
    {
        return getWriter(schema, storage, outputPath, encoding, false, -1, null, null, false);
    }

    public static PixelsWriter getWriter(TypeDescription schema, Storage storage,
                                         String outputPath, boolean encoding,
                                         boolean isPartitioned, int partitionId, List<Integer> keyColumnIds)
    {
        return getWriter(schema, storage, outputPath, encoding, isPartitioned, partitionId, keyColumnIds, null, false);
    }

    public static PixelsWriter getWriter(TypeDescription schema, Storage storage,
                                         String outputPath, boolean encoding,
                                         boolean isPartitioned, int partitionId,
                                         List<Integer> keyColumnIds,
                                         List<String> outputPaths, boolean isSchemaWriter)
    {
        // root operator是可以把结果直接传回VM的，但没时间写了，本次demo时只能把结果写到minio中。所以仍然是
        //  分类讨论，可能返回PixelsWriterStreamImpl或者PixelsWriterImpl
        if (storage != null && storage.getScheme() != Storage.Scheme.mock) return WorkerCommon.getWriter(schema, storage, outputPath, encoding, isPartitioned, keyColumnIds);
        // It's a bad practice to use null to indicate streaming mode. todo: declare a new Storage indicating streaming mode in place of null
        logger.debug("getWriter streaming mode, path: " + outputPath + ", paths: " + outputPaths + ", isSchemaWriter: " + isSchemaWriter);
        requireNonNull(schema, "schema is null");
        requireNonNull(outputPath, "fileName is null");
        checkArgument(!isPartitioned || keyColumnIds != null,
                "keyColumnIds is null whereas isPartitioned is true");
        checkArgument(!isPartitioned || outputPaths != null,
                "outputPaths is null whereas isPartitioned is true");

        PixelsWriterStreamImpl.Builder builder = PixelsWriterStreamImpl.newBuilder();
        builder.setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                // .setOverwrite(true) // set overwrite to true to avoid existence checking.
                .setEncodingLevel(EncodingLevel.EL2) // it is worth to do encoding
                .setPartitioned(isPartitioned)
                .setPartitionId(isPartitioned ? partitionId : -1);
        if (!isPartitioned) {
            if (isSchemaWriter) builder.setUri(URI.create("http://localhost:" + getSchemaPort(outputPath) + "/"));
            builder.setFileName(outputPath);
        }
        else {
            builder.setFileNames(outputPaths)
                    .setPartKeyColumnIds(keyColumnIds);
        }
        return builder.build();
    }

//    public static PixelsReaderOption getReaderOption(long transId, String[] cols, InputInfo input)
//    {
//        return WorkerCommon.getReaderOption(transId, cols, input);
//    }
}
