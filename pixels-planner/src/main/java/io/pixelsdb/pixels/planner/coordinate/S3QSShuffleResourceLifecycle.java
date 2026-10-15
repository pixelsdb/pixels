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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
    private final List<OwnedQueue> ownedQueues = new ArrayList<>();
    private final Set<String> ownedObjectPrefixes = new LinkedHashSet<>();
    private S3QS s3qs;
    private boolean prepared;
    private boolean cleaned;

    @Override
    public synchronized void prepare(Collection<ShuffleInfo> shuffleInfos) throws IOException
    {
        checkState(!prepared, "shuffle resources have already been prepared");
        prepared = true;
        if (shuffleInfos == null || shuffleInfos.isEmpty())
        {
            return;
        }

        try
        {
            for (ShuffleInfo shuffleInfo : shuffleInfos)
            {
                if (shuffleInfo.getStorageInfo() == null ||
                        shuffleInfo.getStorageInfo().getScheme() != Storage.Scheme.s3qs)
                {
                    continue;
                }
                S3QS storage = getS3QS();
                ownedObjectPrefixes.add(shuffleInfo.getObjectPathPrefix());
                for (ShuffleQueueInfo queue : shuffleInfo.getQueues())
                {
                    String queueUrl = queue.getQueueUrl();
                    if (queueUrl == null || queueUrl.trim().isEmpty())
                    {
                        queueUrl = storage.createQueue(queue.getQueueName());
                        queue.setQueueUrl(queueUrl);
                        ownedQueues.add(new OwnedQueue(
                                shuffleInfo.getShuffleId(), queue.getPartitionId(), queueUrl));
                    }
                }
            }
        }
        catch (IOException | RuntimeException e)
        {
            try
            {
                cleanup();
            }
            catch (IOException cleanupFailure)
            {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
    }

    @Override
    public synchronized void cleanup() throws IOException
    {
        if (cleaned)
        {
            return;
        }
        cleaned = true;
        IOException failure = null;

        if (s3qs != null)
        {
            for (OwnedQueue queue : ownedQueues)
            {
                try
                {
                    s3qs.deleteQueue(queue.queueUrl);
                }
                catch (IOException e)
                {
                    failure = addFailure(failure, e);
                }
                try
                {
                    s3qs.unregisterQueue(queue.shuffleId, queue.partitionId);
                }
                catch (IOException e)
                {
                    failure = addFailure(failure, e);
                }
            }
            for (String objectPrefix : ownedObjectPrefixes)
            {
                try
                {
                    s3qs.delete(objectPrefix, true);
                }
                catch (IOException e)
                {
                    failure = addFailure(failure, e);
                }
            }
        }

        if (failure != null)
        {
            throw failure;
        }
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
        private final String queueUrl;

        private OwnedQueue(String shuffleId, int partitionId, String queueUrl)
        {
            this.shuffleId = shuffleId;
            this.partitionId = partitionId;
            this.queueUrl = queueUrl;
        }
    }
}
