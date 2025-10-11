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
import org.junit.jupiter.api.BeforeAll;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Test using the actual index load and update during Retina runtime.
 * Primarily uses the LocalIndexService's deletePrimaryIndexEntry and putPrimaryIndexEntry interfaces.
 * PUT format: tableId, indexId, key, timestamp, rowId, fileId, rgId, rgRowOffset
 * DEL format: tableId, indexId, key, timestamp
 * Note that the key is an integer here.
 */
public class TestRetinaTrace
{
    // Trace load path: only put operations
    private static final String loadPath = "/home/gengdy/data/index/index.load.trace";

    // Trace update path: both put and delete operations
    private static final String updatePath = "/home/gengdy/data/index/index.update.trace";

    private static final IndexService indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local);

    @BeforeAll
    public static void dataPrepare()
    {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(loadPath)))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] parts = line.split("\\t");

                PutOperation putOperation = new PutOperation(parts);
                putOperation.execute();
            }
        } catch (IOException e)
        {
            throw new RuntimeException("Failed to prepare data from loadPath", e);
        } catch (NumberFormatException e)
        {
            throw new RuntimeException("Malformed number in load trace", e);
        }
    }

    private interface TraceOperation
    {
        void execute();
    }

    private static class PutOperation implements TraceOperation
    {
        final long tableId, indexId, timestamp, rowId, fileId;
        final ByteString key;
        final int rgId, rgRowOffset;

        PutOperation(String[] parts)
        {
            if (parts.length != 9)
            {
                throw new RuntimeException("Invalid PUT operation: " + String.join("\t", parts));
            }
            this.tableId = Long.parseLong(parts[1]);
            this.indexId = Long.parseLong(parts[2]);
            this.key = ByteString.copyFrom(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(Integer.parseInt(parts[3])).array());
            this.timestamp = Long.parseLong(parts[4]);
            this.rowId = Long.parseLong(parts[5]);
            this.fileId = Long.parseLong(parts[6]);
            this.rgId = Integer.parseInt(parts[7]);
            this.rgRowOffset = Integer.parseInt(parts[8]);
        }

        @Override
        public void execute()
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

            IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(indexKey)
                    .setRowId(rowId)
                    .setRowLocation(rowLocation)
                    .build();

            try
            {
                indexService.putPrimaryIndexEntry(entry);
            } catch (IndexException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static class DeleteOperation implements TraceOperation
    {
        final long tableId, indexId, timestamp;
        final ByteString key;

        DeleteOperation(String[] parts)
        {
            if (parts.length != 5)
            {
                throw new RuntimeException("Invalid DEL operation: " + String.join("\t", parts));
            }
            this.tableId = Long.parseLong(parts[1]);
            this.indexId = Long.parseLong(parts[2]);
            this.key = ByteString.copyFrom(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(Integer.parseInt(parts[3])).array());
            this.timestamp = Long.parseLong(parts[4]);
        }

        @Override
        public void execute()
        {
            IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder()
                    .setTableId(tableId)
                    .setIndexId(indexId)
                    .setKey(key)
                    .setTimestamp(timestamp)
                    .build();

            try
            {
                indexService.deletePrimaryIndexEntry(indexKey);
            } catch (IndexException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void testIndex()
    {
        System.out.println("Loading trace file into memory...");
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

        System.out.println("Finished loading " + operations.size() + " operations.");

        System.out.println("\nStarting index performance test...");
        long startTime = System.currentTimeMillis();
        for (TraceOperation op : operations)
        {
            op.execute();
        }
        long endTime = System.currentTimeMillis();

        long totalDurationNanos = endTime - startTime;
        double totalDurationSeconds = totalDurationNanos / 1_000_000_000.0;
        long totalOps = putCount + delCount;

        double putThroughput = (totalDurationSeconds > 0) ? (putCount / totalDurationSeconds) : 0;
        double deleteThroughput = (totalDurationSeconds > 0) ? (delCount / totalDurationSeconds) : 0;
        double totalThroughput = (totalDurationSeconds > 0) ? (totalOps / totalDurationSeconds) : 0;

        System.out.println("\n--- Index Performance Test Results ---");
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
    }
}
