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
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/** Handler-owned leases; remote queues and clients keep their existing query/storage ownership. */
final class S3QSQueueScope
{
    private static final Logger logger = LogManager.getLogger(S3QSQueueScope.class);
    private final List<Runnable> releases = new ArrayList<>();
    private final List<CompletableFuture<Void>> owners = new ArrayList<>();
    private boolean closed;

    void own(S3QSAsyncPartitionOutput output)
    {
        if (closed) { throw new IllegalStateException("Queue scope is closed"); }
        if (output != null)
        {
            CompletableFuture<Void> stopped = new CompletableFuture<>();
            owners.add(stopped);
            output.afterTermination(() -> stopped.complete(null));
        }
    }

    void own(S3QSAsyncTasks tasks)
    {
        if (closed) { throw new IllegalStateException("Queue scope is closed"); }
        CompletableFuture<Void> stopped = new CompletableFuture<>();
        owners.add(stopped);
        tasks.afterTermination(() -> stopped.complete(null));
    }

    /** ACK pools hold only the partition they drain, even if handler cleanup times out. */
    void retain(S3QS storage, String shuffle, int partition) throws IOException
    {
        retain(storage, shuffle, partition, null, null);
    }

    private String retain(S3QS storage, String shuffle, int partition, String name, String url) throws IOException
    {
        if (closed) { throw new IllegalStateException("Queue scope is closed"); }
        String resolved = storage.retainQueue(shuffle, partition, name, url);
        releases.add(() -> {
            try { storage.releaseQueue(shuffle, partition); }
            catch (IOException failure) { throw new WorkerException("Failed to release " + shuffle + "/" + partition, failure); }
        });
        return resolved;
    }

    void register(ShuffleInfo info) throws IOException
    {
        if (closed) { throw new IllegalStateException("Queue scope is closed"); }
        if (info == null) { return; }
        WorkerCommon.initStorage(info.getStorageInfo());
        if (info.getStorageInfo().getScheme() != Storage.Scheme.s3qs) { return; }
        if (info.getNumPartitions() <= 0 || info.getQueues() == null ||
                info.getQueues().size() != info.getNumPartitions())
        {
            throw new IOException("Invalid shuffle queue count");
        }
        S3QS storage = (S3QS) WorkerCommon.getStorage(Storage.Scheme.s3qs);
        for (ShuffleQueueInfo queue : info.getQueues())
        {
            if (queue == null || queue.getPartitionId() < 0 || queue.getPartitionId() >= info.getNumPartitions())
            {
                throw new IOException("Invalid shuffle queue partition");
            }
            int partition = queue.getPartitionId();
            String shuffle = info.getShuffleId();
            queue.setQueueUrl(retain(storage, shuffle, partition, queue.getQueueName(), queue.getQueueUrl()));
        }
    }

    void closeAfter(ExecutorService executor)
    {
        if (closed) { return; }
        if (executor != null && !(executor instanceof WorkerThreadPool))
        {
            throw new IllegalArgumentException("Queue scope requires a worker-owned thread pool");
        }
        closed = true;
        if (executor != null)
        {
            CompletableFuture<Void> stopped = new CompletableFuture<>();
            owners.add(stopped);
            ((WorkerThreadPool) executor).afterTermination(() -> stopped.complete(null));
        }
        CompletableFuture.allOf(owners.toArray(new CompletableFuture<?>[0])).thenRun(() -> {
            // A failed close must not strand the remaining local registrations.
            for (Runnable release : releases)
            {
                try { release.run(); }
                catch (RuntimeException failure) { logger.error("Local shuffle queue cleanup failed", failure); }
            }
            releases.clear();
            owners.clear();
        });
        if (executor != null) { executor.shutdownNow(); }
    }
}
