/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.common.utils;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * Unified checkpoint file read/write utility class.
 * <p>
 * Checkpoint file format:
 * <pre>
 * [4B] rgCount                         -- total number of entries
 * Repeated rgCount times:
 *   [8B] fileId                        -- file ID
 *   [4B] rgId                          -- Row Group ID
 *   [4B] recordNum                     -- number of records
 *   [4B] bitmapLen                     -- length of bitmap array
 *   [8B x bitmapLen] bitmap            -- visibility bitmap data
 * </pre>
 * <p>
 * This class is shared by the server-side (RetinaResourceManager) and the client-side
 * (VisibilityCheckpointCache) to eliminate duplicated read/write logic and ensure
 * consistent file format across both sides.
 */
public class CheckpointFileIO
{
    private static final Logger logger = LogManager.getLogger(CheckpointFileIO.class);

    /**
     * A checkpoint entry containing the visibility information of a single Row Group.
     */
    public static class CheckpointEntry
    {
        public final long fileId;
        public final int rgId;
        public final int recordNum;
        public final long[] bitmap;

        public CheckpointEntry(long fileId, int rgId, int recordNum, long[] bitmap)
        {
            this.fileId = fileId;
            this.rgId = rgId;
            this.recordNum = recordNum;
            this.bitmap = bitmap;
        }
    }

    /**
     * Functional interface for processing each CheckpointEntry during parallel reading.
     */
    @FunctionalInterface
    public interface EntryConsumer
    {
        void accept(CheckpointEntry entry);
    }

    private CheckpointFileIO()
    {
    }

    /**
     * Write checkpoint entries to a file.
     * <p>
     * Uses a producer-consumer pattern: the caller puts CheckpointEntry objects into the queue,
     * and this method consumes totalRgs entries from the queue and writes them sequentially.
     *
     * @param path      the file path
     * @param totalRgs  total number of entries to write
     * @param queue     blocking queue containing CheckpointEntry objects
     * @throws Exception if writing fails
     */
    public static void writeCheckpoint(String path, int totalRgs, BlockingQueue<CheckpointEntry> queue) throws Exception
    {
        Storage storage = StorageFactory.Instance().getStorage(path);
        try (DataOutputStream out = storage.create(path, true, 8 * 1024 * 1024))
        {
            out.writeInt(totalRgs);
            for (int i = 0; i < totalRgs; i++)
            {
                CheckpointEntry entry = queue.take();
                out.writeLong(entry.fileId);
                out.writeInt(entry.rgId);
                out.writeInt(entry.recordNum);
                out.writeInt(entry.bitmap.length);
                for (long l : entry.bitmap)
                {
                    out.writeLong(l);
                }
            }
            out.flush();
        }
    }

    /**
     * Read and parse a checkpoint file in parallel using the specified executor.
     *
     * @param path     the file path
     * @param consumer callback for each parsed CheckpointEntry
     * @param executor the executor service for parallel parsing
     * @return         the rgCount (total number of entries) in the file
     * @throws IOException if reading fails
     */
    public static int readCheckpointParallel(String path, EntryConsumer consumer, ExecutorService executor) throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(path);
        long fileLength = storage.getStatus(path).getLength();

        // Step 1: Read the entire file into byte[] at once
        byte[] content;
        try (PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, path))
        {
            ByteBuffer buffer = reader.readFully((int) fileLength);
            if (buffer.hasArray())
            {
                content = buffer.array();
            } else
            {
                content = new byte[(int) fileLength];
                buffer.get(content);
            }
        }

        ByteBuffer buf = ByteBuffer.wrap(content);
        int rgCount = buf.getInt();

        if (rgCount > 0)
        {
            // Step 2: Sequential scan to build offset index (lightweight, pointer jumps only)
            int[] offsets = new int[rgCount];
            for (int i = 0; i < rgCount; i++)
            {
                offsets[i] = buf.position();
                buf.position(buf.position() + 8 + 4 + 4); // skip fileId(8) + rgId(4) + recordNum(4)
                int bitmapLen = buf.getInt();
                buf.position(buf.position() + bitmapLen * 8); // skip bitmap data
            }

            // Step 3: Parallel parsing
            int parallelism = Math.min(Runtime.getRuntime().availableProcessors(),
                    Math.max(1, rgCount / 64));
            int batchSize = (rgCount + parallelism - 1) / parallelism;
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int batchStart = 0; batchStart < rgCount; batchStart += batchSize)
            {
                int start = batchStart;
                int end = Math.min(batchStart + batchSize, rgCount);
                futures.add(CompletableFuture.runAsync(() -> {
                    for (int i = start; i < end; i++)
                    {
                        // Each thread uses its own ByteBuffer instance (sharing the same byte[], thread-safe)
                        ByteBuffer slice = ByteBuffer.wrap(content);
                        slice.position(offsets[i]);
                        long fileId = slice.getLong();
                        int rgId = slice.getInt();
                        int recordNum = slice.getInt();
                        int len = slice.getInt();
                        long[] bitmap = new long[len];
                        for (int j = 0; j < len; j++)
                        {
                            bitmap[j] = slice.getLong();
                        }
                        consumer.accept(new CheckpointEntry(fileId, rgId, recordNum, bitmap));
                    }
                }, executor));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }

        return rgCount;
    }

    /**
     * Read and parse a checkpoint file in parallel using the default ForkJoinPool.commonPool().
     *
     * @param path     the file path
     * @param consumer callback for each parsed CheckpointEntry
     * @return         the rgCount (total number of entries) in the file
     * @throws IOException if reading fails
     */
    public static int readCheckpointParallel(String path, EntryConsumer consumer) throws IOException
    {
        return readCheckpointParallel(path, consumer, ForkJoinPool.commonPool());
    }
}
