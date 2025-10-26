/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.index.rocksdb;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.common.index.IndexServiceProvider;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test using the actual index load and update during Retina runtime.
 * Primarily uses the LocalIndexService's deletePrimaryIndexEntry and putPrimaryIndexEntry interfaces.
 * PUT format: tableId, indexId, key, timestamp, rowId, fileId, rgId, rgRowOffset
 * DEL format: tableId, indexId, key, timestamp
 * *****************************************************************************
 * Note
 * 1. The key is an integer here.
 * 2. Launching the metadataService separately in tests is cumbersome;
 *    it's best to start the pixels-related services externally.
 * 3. Insert records used for tracing into the SINGLE_POINT_INDICES table
 *    in MySQL pixels_metadata.
 *    e.g.,
 *    ```sql
 *    INSERT INTO SINGLE_POINT_INDICES
 *      (SPI_ID, SPI_KEY_COLUMNS, SPI_PRIMARY, SPI_UNIQUE, SPI_INDEX_SCHEME, TBLS_TBL_ID, SCHEMA_VERSIONS_SV_ID)
 *    VALUES
 *      (403, '{"keyColumnIds":[403]}', 1, 1, 'rocksdb', 3, 3),
 *      (404, '{"keyColumnIds":[404]}', 1, 1, 'rocksdb', 3, 3);
 *    ```
 * *****************************************************************************
 */
public class TestRetinaTrace
{
    private static final int THREAD_COUNT = 16;

    // Trace load path: only put operations
    private static final String loadPath = "/home/gengdy/data/index/index.load.trace";

    // Trace update path: both put and delete operations
    private static final String updatePath = "/home/gengdy/data/index/index.update.trace";

    private static final IndexService indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local);

    /**
     * Load the initial data into the index
     */
    @BeforeAll
    public static void prepare()
    {
        System.out.println("Preparing data from loadPath into index...");
        long count = 0;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(loadPath)))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                count++;
                String[] parts = line.split("\\t");
                PutOperation putOperation = new PutOperation(parts);
                IndexProto.PrimaryIndexEntry entry = (IndexProto.PrimaryIndexEntry) putOperation.toProto();
                indexService.putPrimaryIndexEntry(entry);
            }
        } catch (IOException e)
        {
            throw new RuntimeException("Failed to prepare data from loadPath", e);
        } catch (IndexException e)
        {
            throw new RuntimeException("Failed to put index entry during prepare", e);
        }
        System.out.println("Finished preparing " + count * THREAD_COUNT + " records into index.");
    }

    private interface TraceOperation
    {
        int getBucket();
        Object toProto();
    }

    private static class PutOperation implements TraceOperation
    {
        final long tableId, indexId, timestamp, rowId, fileId;
        final ByteString key;
        final int rgId, rgRowOffset;
        final int bucket;

        PutOperation(String[] parts)
        {
            if (parts.length != 9)
            {
                throw new RuntimeException("Invalid PUT operation: " + String.join("\t", parts));
            }
            this.tableId = Long.parseLong(parts[1]);
            this.indexId = Long.parseLong(parts[2]);
            int keyInt = Integer.parseInt(parts[3]);
            this.key = ByteString.copyFrom(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(keyInt).array());
            this.timestamp = Long.parseLong(parts[4]);
            this.rowId = Long.parseLong(parts[5]);
            this.fileId = Long.parseLong(parts[6]);
            this.rgId = Integer.parseInt(parts[7]);
            this.rgRowOffset = Integer.parseInt(parts[8]);
            this.bucket = keyInt % THREAD_COUNT;
        }

        @Override
        public int getBucket()
        {
            return bucket;
        }

        @Override
        public Object toProto()
        {
            IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder()
                    .setTableId(tableId)
                    .setIndexId(indexId)
                    .setKey(key)
                    .setTimestamp(timestamp)
                    .build();

            IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                    .setFileId(fileId)
                    .setRgId(rgId)
                    .setRgRowOffset(rgRowOffset)
                    .build();

            return IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(indexKey)
                    .setRowId(rowId)
                    .setRowLocation(rowLocation)
                    .build();
        }
    }

    private static class DeleteOperation implements TraceOperation
    {
        final long tableId, indexId, timestamp;
        final ByteString key;
        final int bucket;

        DeleteOperation(String[] parts)
        {
            if (parts.length != 5)
            {
                throw new RuntimeException("Invalid DEL operation: " + String.join("\t", parts));
            }
            this.tableId = Long.parseLong(parts[1]);
            this.indexId = Long.parseLong(parts[2]);
            int keyInt = Integer.parseInt(parts[3]);
            this.key = ByteString.copyFrom(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(keyInt).array());
            this.timestamp = Long.parseLong(parts[4]);
            this.bucket = keyInt % THREAD_COUNT;
        }

        @Override
        public int getBucket()
        {
            return bucket;
        }

        @Override
        public Object toProto()
        {
            return IndexProto.IndexKey.newBuilder()
                    .setTableId(tableId)
                    .setIndexId(indexId)
                    .setKey(key)
                    .setTimestamp(timestamp)
                    .build();
        }
    }

    private static class IndexWorker implements Runnable
    {
        private final List<Object> protoOperations;

        public IndexWorker(List<Object> protoOperations)
        {
            this.protoOperations = protoOperations;
        }

        @Override
        public void run()
        {
            try
            {
                for (Object proto : protoOperations)
                {
                    if (proto instanceof IndexProto.PrimaryIndexEntry)
                    {
                        indexService.putPrimaryIndexEntry((IndexProto.PrimaryIndexEntry) proto);
                    } else
                    {
                        indexService.deletePrimaryIndexEntry((IndexProto.IndexKey) proto);
                    }
                }
            } catch (IndexException e)
            {
                throw new RuntimeException("Index operation failed in worker thread", e);
            }
        }
    }

    @Test
    public void testIndex()
    {
        System.out.println("pid: " + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        System.out.println("Loading baseTrace...");
        List<TraceOperation> operations = new ArrayList<>();
        long putCount = 0, delCount = 0;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(updatePath)))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] parts = line.split("\\t");
                if (parts.length < 1)
                {
                    continue;
                }
                String opType = parts[0];
                if (opType.equals("P"))
                {
                    putCount++;
                    operations.add(new PutOperation(parts));
                } else if (opType.equals("D"))
                {
                    delCount++;
                    operations.add(new DeleteOperation(parts));
                } else
                {
                    throw new RuntimeException("Unknown operation type: " + opType);
                }
            }
        } catch (IOException e)
        {
            throw new RuntimeException("Failed to read update trace file", e);
        } catch (NumberFormatException e)
        {
            throw new RuntimeException("Malformed number in update trace", e);
        }
        System.out.println("Loaded " + operations.size() + " operations from update trace.");

        System.out.println("Generating workloads for " + THREAD_COUNT + " threads...");
        List<List<Object>> threadProtoOperations = IntStream.range(0, THREAD_COUNT)
                .mapToObj(i -> new ArrayList<Object>())
                .collect(Collectors.toList());

        for (TraceOperation op : operations)
        {
            threadProtoOperations.get(op.getBucket()).add(op.toProto());
        }
        System.out.println("Finished pre-building protobuf objects.");

        System.out.println("Starting index performance test with " + THREAD_COUNT + " threads...");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<?>> futures = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        for (List<Object> threadProtoOps : threadProtoOperations)
        {
           futures.add(executor.submit(new IndexWorker(threadProtoOps)));
        }
        for (Future<?> f : futures)
        {
            try
            {
                f.get();
            } catch (Exception e)
            {
                throw new RuntimeException("Thread execution failed", e);
            }
        }
        long endTime = System.currentTimeMillis();

        long totalDurationNanos = endTime - startTime;
        double totalDurationSeconds = totalDurationNanos / 1000.0;
        long totalOps = putCount + delCount;

        double putThroughput = (totalDurationSeconds > 0) ? (putCount / totalDurationSeconds) : 0;
        double deleteThroughput = (totalDurationSeconds > 0) ? (delCount / totalDurationSeconds) : 0;
        double totalThroughput = (totalDurationSeconds > 0) ? (totalOps / totalDurationSeconds) : 0;

        System.out.println("\n--- Index Performance Test Results ---");
        System.out.printf("Thread Count: %d, Mode: Single Entry\n", THREAD_COUNT);
        System.out.printf("Total test time: %.3f seconds\n", totalDurationSeconds);
        System.out.println("------------------------------------");
        System.out.printf("Total PUT operations:    %,d\n", putCount);
        System.out.printf("Total DELETE operations: %,d\n", delCount);
        System.out.printf("Total operations:        %,d\n", totalOps);
        System.out.println("------------------------------------");
        System.out.printf("PUT throughput:          %,.2f ops/sec\n", putThroughput);
        System.out.printf("DELETE throughput:       %,.2f ops/sec\n", deleteThroughput);
        System.out.printf("Total throughput:        %,.2f ops/sec\n", totalThroughput);
        System.out.println("------------------------------------\n");

        indexService.printCacheHint();
    }
}
