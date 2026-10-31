/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common.physical.scheduler;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Scheduler;
import io.pixelsdb.pixels.common.transaction.TransContextCache;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;

/**
 * SortMerge scheduler firstly sorts the requests in the batch by the start offset,
 * then it tries to merge the requests that can be read sequentially from the reader.
 * @author hank
 * @create 2021-09-12
 */
public class SortMergeScheduler implements Scheduler
{
    private static final Logger logger = LogManager.getLogger(SortMergeScheduler.class);

    private static final class InstanceHolder
    {
        private static final SortMergeScheduler instance = new SortMergeScheduler();
    }
    private static int MaxGap;

    public static Scheduler Instance()
    {
        return InstanceHolder.instance;
    }

    protected RetryPolicy retryPolicy;
    protected final boolean enableRetry;

    protected SortMergeScheduler()
    {
        this.enableRetry = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("read.request.enable.retry"));
        if (this.enableRetry)
        {
            int interval = Integer.parseInt(ConfigFactory.Instance().getProperty("read.request.retry.interval.ms"));
            this.retryPolicy = new RetryPolicy(interval);
        }
    }

    static
    {
        ConfigFactory.Instance().registerUpdateCallback("read.request.merge.gap", value ->
                MaxGap = Integer.parseInt(value));
        MaxGap = Integer.parseInt(ConfigFactory.Instance().getProperty("read.request.merge.gap"));
    }

    /**
     * Sort the requests in the batch by their start offsets, and try to merge them.
     * @param batch the request batch.
     * @param transId the transaction id.
     * @return the merged requests.
     */
    protected List<MergedRequest> sortMerge(RequestBatch batch, long transId)
    {
        /**
         * Issue #175:
         * It has been proved that per-query scheduling, i.e., gradually increase
         * the maximum size of merged requests does not provide any performance gain.
         * On the opposite, it decreases the performance of TPC-H (SF=100) by 5-10%.
         * Tuning the split size of Presto can solve the slow-starting issue of
         * compact layout of TPC-H.
         */
        List<CompletableFuture<ByteBuffer>> futures = batch.getFutures();
        List<Request> requests = batch.getRequests();
        List<RequestFuture> requestFutures = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); ++i)
        {
            requestFutures.add(new RequestFuture(requests.get(i), futures.get(i)));
        }
        Collections.sort(requestFutures);
        List<MergedRequest> mergedRequests = new ArrayList<>();
        MergedRequest mr1 = new MergedRequest(requestFutures.get(0));
        MergedRequest mr2 = mr1;
        for (int i = 1; i < batch.size(); ++i)
        {
            mr2 = mr1.merge(requestFutures.get(i));
            if (mr1 == mr2)
            {
                continue;
            }
            mergedRequests.add(mr1);
            mr1 = mr2;
        }
        mergedRequests.add(mr2);

        return mergedRequests;
    }

    @Override
    public void executeBatch(PhysicalReader reader, RequestBatch batch, long transId) throws IOException
    {
        if (batch.size() <= 0)
        {
            return;
        }

        List<MergedRequest> mergedRequests = sortMerge(batch, transId);

        if (reader.supportsAsync())
        {
            for (MergedRequest merged : mergedRequests)
            {
                merged.startTimeMs = System.currentTimeMillis();
                merged.submit(reader);
                if (enableRetry && reader.supportsAsyncRetry())
                {
                    this.retryPolicy.monitor(new MergedExecutableRequest(merged, reader));
                }
            }
        }
        else
        {
            for (MergedRequest merged : mergedRequests)
            {
                reader.seek(merged.getStart());
                ByteBuffer buffer = reader.readFully(merged.getLength());
                merged.complete(buffer);
            }
        }
    }

    protected static class RequestFuture implements Comparable<RequestFuture>
    {
        public Request request;
        public CompletableFuture<ByteBuffer> future;

        public RequestFuture(Request request, CompletableFuture<ByteBuffer> future)
        {
            this.request = request;
            this.future = future;
        }

        @Override
        public int compareTo(RequestFuture o)
        {
            return this.request.compareTo(o.request);
        }
    }

    protected static class MergedRequest
    {
        private final long transId;
        private final long start;
        private long end;
        private int length; // the length of merged request.
        private int size; // the number of sub-requests.
        /**
         * The starting offset of the sub-requests in the response of the merged request.
         */
        private final List<Integer> offsets;
        /**
         * The length of the sub-requests,
         */
        private final List<Integer> lengths;
        private final List<CompletableFuture<ByteBuffer>> futures;
        // fields used by the retry policy.
        protected volatile long startTimeMs = -1;
        protected volatile long completeTimeMs = -1;
        private int retried = 0;
        private final AtomicBoolean done = new AtomicBoolean();
        private final AtomicBoolean watching = new AtomicBoolean();
        private final Set<CompletableFuture<ByteBuffer>> reads = ConcurrentHashMap.newKeySet();

        public MergedRequest(RequestFuture first)
        {
            this.transId = first.request.transId;
            this.start = first.request.start;
            this.end = first.request.start + first.request.length;
            this.offsets = new ArrayList<>();
            this.lengths = new ArrayList<>();
            this.futures = new ArrayList<>();
            this.offsets.add(0);
            this.lengths.add(first.request.length);
            this.length = first.request.length;
            this.futures.add(first.future);
            this.size = 1;
        }

        public MergedRequest merge(RequestFuture curr)
        {
            if (curr.request.start < this.end)
            {
                throw new IllegalArgumentException("Can not merge backward request.");
            }
            if (curr.request.transId != this.transId)
            {
                throw new IllegalArgumentException("Can not merge requests from different queries (transactions).");
            }
            long gap = (curr.request.start - this.end);
            if (gap <= MaxGap && (this.length + gap + curr.request.length) <= Integer.MAX_VALUE)
            {
                this.offsets.add(this.length + (int) gap);
                this.lengths.add(curr.request.length);
                this.length += (gap + curr.request.length);
                this.end = curr.request.start + curr.request.length;
                this.futures.add(curr.future);
                this.size ++;
                return this;
            }
            return new MergedRequest(curr);
        }

        private void watch(CompletableFuture<ByteBuffer> chunk)
        {
            chunk.whenComplete((value, failure) -> {
                if (failure != null) { fail(failure); }
            });
        }

        void submit(PhysicalReader reader)
        {
            // Merging must finish before callbacks can traverse the chunk list.
            // Also handles chunks cancelled before executeBatch was called.
            if (watching.compareAndSet(false, true)) { futures.forEach(this::watch); }
            if (done.get()) { return; }
            try
            {
                CompletableFuture<ByteBuffer> read = reader.readAsync(start, length);
                reads.add(read);
                // Cancellation can race with admission or SDK submission.
                if (done.get()) { read.cancel(true); reads.remove(read); return; }
                read.whenComplete((buffer, failure) -> {
                    reads.remove(read);
                    if (failure != null) { fail(failure); }
                    else { complete(buffer); }
                });
            }
            catch (Exception failure) { fail(failure); }
        }

        private void fail(Throwable failure)
        {
            if (!done.compareAndSet(false, true)) { return; }
            completeTimeMs = System.currentTimeMillis();
            futures.forEach(chunk -> chunk.completeExceptionally(failure));
            cancelReads();
        }

        private void cancelReads()
        {
            reads.forEach(read -> read.cancel(true));
            reads.clear();
        }

        public long getTransId()
        {
            return transId;
        }

        public long getStart()
        {
            return start;
        }

        /**
         * The length in bytes of this merged request.
         * @return
         */
        public int getLength()
        {
            return this.length;
        }

        /**
         * The number of the origin requests that are merged here.
         * @return
         */
        public int getSize()
        {
            return size;
        }

        /**
         * When the data has been read, complete all the futures.
         * @param buffer the data that has been read.
         */
        public void complete(ByteBuffer buffer)
        {
            if (buffer == null || buffer.remaining() < length)
            {
                fail(new IOException("Null or short read at " + start + ", expected " + length + " bytes"));
                return;
            }
            if (!done.compareAndSet(false, true)) { return; }
            completeTimeMs = System.currentTimeMillis();
            for (int i = 0; i < size; i++)
            {
                // Each chunk owns its view; do not mutate the backend's buffer cursor.
                ByteBuffer chunk = buffer.duplicate();
                chunk.position(buffer.position() + offsets.get(i));
                chunk.limit(chunk.position() + lengths.get(i));
                futures.get(i).complete(chunk.slice());
            }
            cancelReads();
        }
    }

    /**
     * Combination of MergedRequest and PhysicalReader. It is used by the RetryPolicy.
     */
    protected static class MergedExecutableRequest implements RetryPolicy.ExecutableRequest
    {
        private final MergedRequest request;
        private final PhysicalReader reader;

        protected MergedExecutableRequest(MergedRequest request, PhysicalReader reader)
        {
            this.request = request;
            this.reader = reader;
        }

        @Override
        public long getStartTimeMs()
        {
            return this.request.startTimeMs;
        }

        @Override
        public long getCompleteTimeMs()
        {
            return this.request.completeTimeMs;
        }

        @Override
        public int getLength()
        {
            return this.request.length;
        }

        @Override
        public int getRetried()
        {
            return this.request.retried;
        }

        @Override
        public boolean execute()
        {
            if (TransContextCache.Instance().isTerminated(request.transId))
            {
                /**
                 * Issue #139:
                 * If the query has been terminated (e.g., canceled or completed), give up retrying.
                 */
                return false;
            }
            this.request.startTimeMs = System.currentTimeMillis();
            this.request.submit(reader);
            this.request.retried++;
            return true;
        }
    }
}
