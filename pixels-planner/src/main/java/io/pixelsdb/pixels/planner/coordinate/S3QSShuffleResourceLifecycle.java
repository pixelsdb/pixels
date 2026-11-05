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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import io.pixelsdb.pixels.common.physical.ObjectPath;
import io.pixelsdb.pixels.common.turbo.cleanup.CleanupResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.function.Consumer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

/**
 * Provisions and cleans the AWS resources owned by one S3QS query execution.
 *
 * Workers still resolve the deterministic queue name locally. Provisioning it
 * here first makes the query execution the owner and gives cleanup an exact
 * list of queue URLs rather than relying on worker-JVM state.
 */
public class S3QSShuffleResourceLifecycle implements ShuffleResourceLifecycle
{
    private static final int PREPARATION_THREADS = 16;
    private final List<OwnedQueue> ownedQueues = new ArrayList<>();
    private final Set<String> ownedObjectPrefixes = new LinkedHashSet<>();
    private final Set<String> allOwnedObjectPrefixes = new LinkedHashSet<>();
    private S3QS s3qs;
    private boolean prepared;
    private boolean cleaned;
    private Consumer<Map<String, Object>> progressListener = ignored -> {};

    @Override
    public synchronized void setProgressListener(Consumer<Map<String, Object>> listener)
    {
        this.progressListener = java.util.Objects.requireNonNull(listener, "listener is null");
        recordProgress();
    }

    S3QSShuffleResourceLifecycle(S3QS s3qs) { this.s3qs = s3qs; }
    public S3QSShuffleResourceLifecycle() {}

    @Override
    public List<CleanupResource> describeResources(Collection<ShuffleInfo> shuffleInfos) throws IOException
    {
        List<CleanupResource> resources = new ArrayList<>();
        for (ShuffleInfo info : shuffleInfos)
        {
            if (info.getStorageInfo() == null || info.getStorageInfo().getScheme() != Storage.Scheme.s3qs) { continue; }
            ObjectPath prefix = new ObjectPath(info.getObjectPathPrefix());
            if (!prefix.valid || prefix.key == null || prefix.key.trim().isEmpty() || !prefix.isFolder)
            { throw new IOException("query shuffle ownership requires a non-root object prefix ending in '/'"); }
            resources.add(new CleanupResource("storage-prefix", "s3://" + prefix.bucket + "/" + prefix.key));
            for (ShuffleQueueInfo queue : info.getQueues())
            {
                if (queue.getQueueUrl() == null || queue.getQueueUrl().trim().isEmpty())
                { resources.add(new CleanupResource("s3qs-queue", queue.getQueueName())); }
            }
        }
        return resources;
    }

    @Override
    public synchronized void closeLocalResources() throws IOException
    {
        if (s3qs == null) { return; }
        for (OwnedQueue queue : ownedQueues) { s3qs.unregisterQueue(queue.shuffleId, queue.partitionId); }
    }

    @Override
    public synchronized void prepare(Collection<ShuffleInfo> shuffleInfos) throws IOException
    {
        checkState(!prepared, "shuffle resources have already been prepared");
        prepared = true;
        if (shuffleInfos == null || shuffleInfos.isEmpty()) { return; }
        List<Callable<Void>> creates = new ArrayList<>();
        for (ShuffleInfo shuffleInfo : shuffleInfos)
        {
            if (shuffleInfo.getStorageInfo() == null ||
                    shuffleInfo.getStorageInfo().getScheme() != Storage.Scheme.s3qs) { continue; }
            S3QS storage = getS3QS();
            ObjectPath prefix = new ObjectPath(shuffleInfo.getObjectPathPrefix());
            if (!prefix.valid || prefix.key == null || prefix.key.trim().isEmpty() || !prefix.isFolder)
            { throw new IOException("query shuffle ownership requires a non-root object prefix ending in '/'"); }
            ownedObjectPrefixes.add(shuffleInfo.getObjectPathPrefix());
            allOwnedObjectPrefixes.add(shuffleInfo.getObjectPathPrefix());
            for (ShuffleQueueInfo queue : shuffleInfo.getQueues())
            {
                if (queue.getQueueUrl() == null || queue.getQueueUrl().trim().isEmpty())
                {
                    OwnedQueue owned = new OwnedQueue(shuffleInfo.getShuffleId(), queue.getPartitionId(),
                            queue.getQueueName());
                    ownedQueues.add(owned);
                    creates.add(() -> {
                        String createdUrl = storage.createQueue(queue.getQueueName());
                        owned.queueUrl = createdUrl;
                        queue.setQueueUrl(createdUrl);
                        return null;
                    });
                }
            }
        }
        // Persist every possible queue name before any create, including lost responses.
        recordProgress();
        prepareQueues(creates);
        recordProgress();
    }

    private static void prepareQueues(List<Callable<Void>> creates) throws IOException
    {
        if (creates.isEmpty()) { return; }
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(PREPARATION_THREADS, creates.size()));
        ExecutorCompletionService<Void> completions = new ExecutorCompletionService<>(executor);
        try
        {
            for (Callable<Void> create : creates) { completions.submit(create); }
            for (int i = 0; i < creates.size(); i++) { completions.take().get(); }
        }
        catch (InterruptedException failure)
        {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while preparing shuffle queues", failure);
        }
        catch (ExecutionException failure)
        {
            Throwable cause = failure.getCause();
            if (cause instanceof IOException) { throw (IOException) cause; }
            throw new IOException("failed to prepare shuffle queues", cause);
        }
        finally
        {
            executor.shutdownNow();
            boolean interrupted = Thread.interrupted();
            // An interrupted Future does not prove that its AWS create has stopped.
            // Do not let cleanup or task publication race a late successful create.
            while (!executor.isTerminated())
            {
                try { executor.awaitTermination(1, TimeUnit.SECONDS); }
                catch (InterruptedException ignored) { interrupted = true; }
            }
            if (interrupted) { Thread.currentThread().interrupt(); }
        }
    }

    @Override
    @Deprecated // Kept for embedded/manual callers; coordinated queries use the shared resource manager.
    public synchronized void cleanup() throws IOException
    {
        if (cleaned)
        {
            return;
        }
        IOException failure = null;

        if (s3qs != null)
        {
            for (OwnedQueue queue : ownedQueues)
            {
                try
                {
                    if (!queue.deleted)
                    {
                        if (queue.queueUrl == null)
                        {
                            try { queue.queueUrl = s3qs.getQueueUrl(queue.queueName); }
                            catch (IOException lookupFailure)
                            {
                                if (!(lookupFailure.getCause() instanceof QueueDoesNotExistException)) { throw lookupFailure; }
                                queue.deleted = true; // A recorded create may never have been submitted.
                            }
                            recordProgress();
                        }
                        if (!queue.deleted) { s3qs.deleteQueue(queue.queueUrl); }
                        queue.deleted = true;
                        recordProgress();
                    }
                }
                catch (IOException | RuntimeException e)
                {
                    failure = addFailure(failure, asIOException(e));
                }
                try
                {
                    if (queue.deleted && !queue.unregistered)
                    {
                        s3qs.unregisterQueue(queue.shuffleId, queue.partitionId);
                        queue.unregistered = true;
                        recordProgress();
                    }
                }
                catch (IOException | RuntimeException e)
                {
                    failure = addFailure(failure, asIOException(e));
                }
            }
            for (String objectPrefix : new ArrayList<>(ownedObjectPrefixes))
            {
                try
                {
                    s3qs.delete(objectPrefix, true);
                    ownedObjectPrefixes.remove(objectPrefix);
                    recordProgress();
                }
                catch (IOException | RuntimeException e)
                {
                    failure = addFailure(failure, asIOException(e));
                }
            }
        }

        if (failure != null)
        {
            throw failure;
        }
        cleaned = true;
        recordProgress();
    }

    private static IOException asIOException(Exception failure)
    {
        return failure instanceof IOException ? (IOException) failure : new IOException(failure);
    }

    private void recordProgress()
    {
        Map<String, Object> resources = new LinkedHashMap<>();
        List<Map<String, Object>> queues = new ArrayList<>();
        for (OwnedQueue queue : ownedQueues)
        {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("shuffleId", queue.shuffleId);
            item.put("partitionId", queue.partitionId);
            item.put("url", queue.queueUrl);
            item.put("name", queue.queueName);
            item.put("deleted", queue.deleted);
            item.put("unregistered", queue.unregistered);
            queues.add(item);
        }
        resources.put("ownedQueues", queues);
        resources.put("ownedObjectPrefixes", new ArrayList<>(allOwnedObjectPrefixes));
        resources.put("remainingOwnedObjectPrefixes", new ArrayList<>(ownedObjectPrefixes));
        resources.put("cleaned", cleaned);
        progressListener.accept(resources);
    }

    private S3QS getS3QS() throws IOException
    {
        if (s3qs == null)
        {
            Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3qs);
            if (!(storage instanceof S3QS))
            {
                throw new IOException("storage of scheme s3qs is not S3QS");
            }
            s3qs = (S3QS) storage;
        }
        return s3qs;
    }

    private static IOException addFailure(IOException current, IOException next)
    {
        if (current == null)
        {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }

    private static class OwnedQueue
    {
        private final String shuffleId;
        private final int partitionId;
        private final String queueName;
        private volatile String queueUrl;
        private boolean deleted;
        private boolean unregistered;

        private OwnedQueue(String shuffleId, int partitionId, String queueName)
        {
            this.shuffleId = shuffleId;
            this.partitionId = partitionId;
            this.queueName = queueName;
        }
    }
}
