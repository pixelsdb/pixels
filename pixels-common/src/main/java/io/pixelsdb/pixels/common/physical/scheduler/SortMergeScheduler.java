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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * SortMerge scheduler firstly sorts the requests in the batch by the start offset,
 * then it tries to merge the requests that can be read sequentially from the reader.
 * Created at: 9/12/21
 * Author: hank
 */
public class SortMergeScheduler implements Scheduler
{
    private static Logger logger = LogManager.getLogger(SortMergeScheduler.class);
    private static SortMergeScheduler instance;
    private static int MaxGap;

    public static Scheduler Instance()
    {
        if (instance == null)
        {
            instance = new SortMergeScheduler();
        }
        return instance;
    }

    private RetryPolicy retryPolicy;
    private final boolean enableRetry;

    protected SortMergeScheduler()
    {
        this.enableRetry = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("read.request.enable.retry"));
        if (this.enableRetry)
        {
            this.retryPolicy = new RetryPolicy(1000);
        }
    }

    static
    {
        ConfigFactory.Instance().registerUpdateCallback("read.request.merge.gap", value ->
                MaxGap = Integer.parseInt(value));
        MaxGap = Integer.parseInt(ConfigFactory.Instance().getProperty("read.request.merge.gap"));
    }

    protected List<MergedRequest> sortMerge(RequestBatch batch)
    {
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
    public void executeBatch(PhysicalReader reader, RequestBatch batch) throws IOException
    {
        if (batch.size() <= 0)
        {
            return;
        }

        List<MergedRequest> mergedRequests = sortMerge(batch);

        if (reader.supportsAsync())
        {
            for (MergedRequest merged : mergedRequests)
            {
                String path = reader.getPath();
                merged.startTimeMs = System.currentTimeMillis();
                reader.readAsync(merged.start, merged.getLength()).thenAccept(resp ->
                {
                    if (resp != null)
                    {
                        merged.completeTimeMs = System.currentTimeMillis();
                        merged.complete(resp);
                    }
                    else
                    {
                        logger.error("Asynchronous read from path '" +
                                path + "' got null response.");
                    }
                });
                if (enableRetry)
                {
                    this.retryPolicy.monitor(merged, reader);
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

    protected class RequestFuture implements Comparable<RequestFuture>
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

    protected class MergedRequest
    {
        private long start;
        private long end;
        private int position;
        private int size;
        private List<Integer> positions;
        private List<Integer> lengths;
        private List<CompletableFuture<ByteBuffer>> futures;
        // fields used by the retry policy.
        protected volatile long startTimeMs = -1;
        protected volatile long completeTimeMs = -1;
        private int retried = 0;

        public MergedRequest(RequestFuture first)
        {
            this.start = first.request.start;
            this.end = first.request.start + first.request.length;
            this.positions = new ArrayList<>();
            this.lengths = new ArrayList<>();
            this.futures = new ArrayList<>();
            this.positions.add(0);
            this.lengths.add(first.request.length);
            this.position = first.request.length;
            this.futures.add(first.future);
            this.size = 1;
        }

        public MergedRequest merge(RequestFuture curr)
        {
            if (curr.request.start < this.end)
            {
                throw new IllegalArgumentException("Can not merge backward request.");
            }
            int gap = (int) (curr.request.start - this.end);
            if (gap <= MaxGap)
            {
                this.positions.add(this.position + gap);
                this.lengths.add(curr.request.length);
                this.position += (gap + curr.request.length);
                this.end = curr.request.start + curr.request.length;
                this.futures.add(curr.future);
                this.size ++;
                return this;
            }
            return new MergedRequest(curr);
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
            return (int) (end - start);
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
            for (int i = 0; i < size; ++i)
            {
                /**
                 * Issue #114:
                 * Limit should be set before position.
                 */
                buffer.limit(positions.get(i) + lengths.get(i));
                buffer.position(positions.get(i));
                futures.get(i).complete(buffer.slice());
            }
        }
    }

    /**
     * Combination of MergedRequest and PhysicalReader,
     * it is used by the RetryPolicy.
     */
    protected class MergedRequestReader
    {
        private MergedRequest request;
        private PhysicalReader reader;

        protected MergedRequestReader(MergedRequest request, PhysicalReader reader)
        {
            this.request = request;
            this.reader = reader;
        }
    }

    /**
     * The retry policy that retries timeout read requests for a given number of times at most.
     * The timeout is determined by a cost model.
     */
    protected class RetryPolicy
    {
        private int maxRetryNum;
        private int intervalMs;
        private final ConcurrentLinkedQueue<MergedRequestReader> requestReaders;
        ThreadGroup monitorThreadGroup;
        private final ExecutorService monitorService;

        private static final int FIRST_BYTE_LATENCY_MS = 1000; // 1000ms
        private static final int TRANSFER_RATE_BPMS = 10240; // 10KB/ms

        protected RetryPolicy(int intervalMs)
        {
            this.maxRetryNum = Integer.parseInt(ConfigFactory.Instance().getProperty("read.request.max.retry.num"));
            this.intervalMs = intervalMs;
            this.requestReaders = new ConcurrentLinkedQueue<>();

            // Issue #133: set the monitor thread as daemon thread with max priority.
            this.monitorThreadGroup = new ThreadGroup("pixels.retry.monitor");
            this.monitorThreadGroup.setMaxPriority(Thread.MAX_PRIORITY);
            this.monitorThreadGroup.setDaemon(true);
            this.monitorService = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(monitorThreadGroup, runnable);
                thread.setDaemon(true);
                thread.setPriority(Thread.MAX_PRIORITY);
                return thread;
            });

            this.monitorService.execute(() ->
            {
                while (true)
                {
                    long currentTimeMs = System.currentTimeMillis();
                    for (Iterator<MergedRequestReader> it = requestReaders.iterator(); it.hasNext(); )
                    {
                        MergedRequestReader requestReader = it.next();
                        MergedRequest request = requestReader.request;
                        if (request.completeTimeMs > 0)
                        {
                            // request has completed.
                            it.remove();
                        } else if (currentTimeMs - request.startTimeMs > timeoutMs(request.getLength()))
                        {
                            if (request.retried >= 3)
                            {
                                // give up retry.
                                it.remove();
                                continue;
                            }
                            // retry request.
                            String path = requestReader.reader.getPath();
                            logger.debug("Retry request: path='" + path + "', start=" +
                                    request.start + ", length=" + request.getLength());
                            try
                            {
                                request.startTimeMs = System.currentTimeMillis();
                                requestReader.reader.readAsync(request.start, request.getLength()).thenAccept(resp ->
                                {
                                    if (resp != null)
                                    {
                                        request.completeTimeMs = System.currentTimeMillis();
                                        request.complete(resp);
                                    }
                                    else
                                    {
                                        logger.error("Asynchronous read from path '" +
                                                path + "' got null response.");
                                    }
                                });
                            } catch (IOException e)
                            {
                                logger.error("Failed to read asynchronously from path '" +
                                        path + "'.");
                            }
                            finally
                            {
                                /**
                                 * The retried request does not be removed here.
                                 * It will be remove when:
                                 * 1. the retry complete on time, or
                                 * 2. the max number of retries has been reached.
                                 */
                                request.retried++;
                            }
                        }
                    }
                    try
                    {
                        // sleep for some time, to release the cpu.
                        Thread.sleep(intervalMs);
                    } catch (InterruptedException e)
                    {
                        logger.error("Retry policy is interrupted during sleep.", e);
                    }
                }
            });

            this.monitorService.shutdown();

            Runtime.getRuntime().addShutdownHook(new Thread(monitorService::shutdownNow));
        }

        private int timeoutMs(int length)
        {
            return FIRST_BYTE_LATENCY_MS + length/TRANSFER_RATE_BPMS;
        }

        protected void monitor(MergedRequest request, PhysicalReader reader)
        {
            this.requestReaders.add(new MergedRequestReader(request, reader));
        }
    }
}
