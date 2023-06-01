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
package io.pixelsdb.pixels.common.physical;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The interface for the reading request schedulers.
 * From each thread, call the addRequest() method to add read
 * requests, and then execute them in a
 * @create 2021-09-10
 * @author hank
 */
public interface Scheduler
{
    /**
     * Execute a batch of read requests, and return the future of the completion of
     * all the requests.
     * @param reader
     * @param batch
     * @param transId
     * @throws IOException
     */
    void executeBatch(PhysicalReader reader, RequestBatch batch, long transId)
            throws IOException;

    class Request implements Comparable<Request>
    {
        public final long transId;
        public final long start;
        public final int length;

        public Request(long transId, long start, int length)
        {
            this.transId = transId;
            this.start = start;
            this.length = length;
        }

        @Override
        public int hashCode()
        {
            return (int) ((this.start<<32)>>32);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof Request)
            {
                Request r = (Request) obj;
                return this.start == r.start &&
                        this.length == r.length;
            }
            return false;
        }

        @Override
        public int compareTo(Request o)
        {
            return Long.compare(this.start, o.start);
        }
    }

    class RequestBatch
    {
        private int size;
        private List<Request> requests;
        private List<CompletableFuture<ByteBuffer>> futures;

        public RequestBatch()
        {
            this.requests = new ArrayList<>();
            this.futures = new ArrayList<>();
            this.size = 0;
        }

        public RequestBatch(int capacity)
        {
            if (capacity <= 0)
            {
                throw new IllegalArgumentException("Request batch capacity: " + capacity);
            }
            this.requests = new ArrayList<>(capacity);
            this.futures = new ArrayList<>(capacity);
            this.size = 0;
        }

        public CompletableFuture<ByteBuffer> add(long transId, long start, int length)
        {
            Request request = new Request(transId, start, length);
            CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
            requests.add(request);
            futures.add(future);
            size++;
            return future;
        }

        public CompletableFuture<ByteBuffer> add(Request request)
        {
            CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
            requests.add(request);
            futures.add(future);
            size++;
            return future;
        }

        public int size()
        {
            return size;
        }

        public List<Request> getRequests()
        {
            return requests;
        }

        public List<CompletableFuture<ByteBuffer>> getFutures()
        {
            return futures;
        }

        /**
         * If batch is empty, this method returns and completed future.
         * @return
         */
        public CompletableFuture<Void> completeAll(List<CompletableFuture> actionFutures)
        {
            assert actionFutures != null;
            assert actionFutures.size() == size;
            if (size <= 0)
            {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.complete(null);
                return future;
            }
            CompletableFuture[] fs = new CompletableFuture[size];
            for (int i = 0; i < size; ++i)
            {
                fs[i] = actionFutures.get(i);
            }
            return CompletableFuture.allOf(fs);
        }

        public void clear()
        {
            this.futures.clear();
            this.requests.clear();
            this.size = 0;
        }
    }
}
