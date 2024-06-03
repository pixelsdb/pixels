/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.worker.common.WorkerCommon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StreamWorkerCommon extends WorkerCommon {
    private static final Logger logger = LogManager.getLogger(StreamWorkerCommon.class);

    public static void initStorage(StorageInfo storageInfo, Boolean isOutput) throws IOException {
        if (storageInfo.getScheme() == Storage.Scheme.mock) {
            // Currently, we use Storage.Scheme.mock to indicate streaming mode,
            //  where we don't need to initialize anything. Returns immediately.
            if (isOutput)
            {
                // This is an output storage using HTTP. The opposite side must be waiting for a schema;
                //  will need to send the schema one-off.
                // But currently this is done in BaseStreamWorkers, so nothing here.

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
            throw new IllegalArgumentException("Attempt to call a streaming mode function with a non-HTTP storage");
        }
        // Start a special port to pass schema
        passSchemaToNextLevel(schema, storageInfo, "http://localhost:" + PixelsWriterStreamImpl.getSchemaPort(outputInfo.getPath()) + "/");
    }

    public static void passSchemaToNextLevel(TypeDescription schema, StorageInfo storageInfo, String endpoint)
            throws IOException {
        if (storageInfo.getScheme() != Storage.Scheme.mock ||
                !Objects.equals(storageInfo.getRegion(), "http")) {
            throw new IllegalArgumentException("Attempt to call a streaming mode function with a non-HTTP storage");
        }
        PixelsWriter pixelsWriter = getWriter(schema, null, endpoint, false, false, -1, null, null, true);
        pixelsWriter.close();  // We utilize the sendRowGroup() in PixelsWriterStreamImpl's close() to send the schema.
    }

    public static void passSchemaToNextLevel(TypeDescription schema, StorageInfo storageInfo, List<String> endpoints)
            throws IOException {
        for (String endpoint : endpoints)
            passSchemaToNextLevel(schema, storageInfo, endpoint);  // Can do it in parallel, but this is not a bottleneck
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
        if (storage == null) {
            PixelsReader pixelsReader = new PixelsReaderStreamImpl(PixelsWriterStreamImpl.getSchemaPort(inputSplits.get(0).getInputInfos().get(0).getPath()));
            TypeDescription ret = pixelsReader.getFileSchema();
            pixelsReader.close();
            return ret;
        }
        return WorkerCommon.getFileSchemaFromSplits(storage, inputSplits);
    }

    public static TypeDescription getSchemaFromPaths(Storage storage, List<String> paths)
            throws Exception {
        if (storage == null)
        {
            PixelsReader pixelsReader = new PixelsReaderStreamImpl(PixelsWriterStreamImpl.getSchemaPort(paths.get(0)));
            TypeDescription ret = pixelsReader.getFileSchema();
            pixelsReader.close();
            return ret;
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
            // streaming mode
            // Currently, the first packet from the stream brings the schema
            Future<?> leftFuture = executor.submit(() -> {
                try {
                    PixelsReader pixelsReader = new PixelsReaderStreamImpl(PixelsWriterStreamImpl.getSchemaPort(leftPaths.get(0)));
                    leftSchema.set(pixelsReader.getFileSchema());
                    pixelsReader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            Future<?> rightFuture = executor.submit(() -> {
                try {
                    PixelsReader pixelsReader = new PixelsReaderStreamImpl(PixelsWriterStreamImpl.getSchemaPort(rightPaths.get(0)));
                    rightSchema.set(pixelsReader.getFileSchema());
                    pixelsReader.close();  // XXX: This `close()` makes the test noticeably slower. Will need to look into it.
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

    public static PixelsReader getReader(String filePath, Storage storage) throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException("Forbidden to call WorkerCommon.getReader() from StringWorkerCommon");
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
            logger.debug("getReader streaming mode, path: " + path + ", port: " + PixelsWriterStreamImpl.getOrSetPort(path));
            return new PixelsReaderStreamImpl("http://localhost:" + PixelsWriterStreamImpl.getOrSetPort(path) + "/", partitioned, numHashes);
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
        if (storage != null && storage.getScheme() != Storage.Scheme.mock) return WorkerCommon.getWriter(schema, storage, outputPath, encoding, isPartitioned, keyColumnIds);
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
                .setEncodingLevel(EncodingLevel.EL2) // it is worth to do encoding
                .setPartitioned(isPartitioned)
                .setPartitionId(isPartitioned ? partitionId : -1);
        if (!isPartitioned) {
            if (isSchemaWriter) builder.setUri(URI.create("http://localhost:" + PixelsWriterStreamImpl.getSchemaPort(outputPath) + "/"));
            builder.setFileName(outputPath);
        }
        else {
            builder.setFileNames(outputPaths)
                    .setPartKeyColumnIds(keyColumnIds);
        }
        return builder.build();
    }
}
