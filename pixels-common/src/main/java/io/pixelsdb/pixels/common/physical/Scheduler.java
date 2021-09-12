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
import java.util.concurrent.CompletableFuture;

/**
 * The interface for the reading request schedulers.
 * From each thread, call the addRequest() method to add read
 * requests, and then execute them in a
 * Created at: 9/10/21
 * Author: hank
 */
public interface Scheduler
{
    CompletableFuture<Void> executeBatch(PhysicalReader reader, RequestBatch batch) throws IOException;

    class Request implements Comparable<Request>
    {
        public long start;
        public int length;

        public Request(long start, int length)
        {
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
            return (int) (this.start - o.start);
        }
    }

    class RequestBatch
    {
        private int pos;
        private int size;
        private Request[] requests;
        private CompletableFuture<ByteBuffer>[] futures;

        @SuppressWarnings("unchecked")
        public RequestBatch(int size)
        {
            if (size <= 0)
            {
                throw new IllegalArgumentException("Request batch size: " + size);
            }
            this.requests = new Request[size];
            this.futures = new CompletableFuture[size];
            this.pos = 0;
        }

        public CompletableFuture<ByteBuffer> add(Request request)
        {
            if (pos >= size)
            {
                throw new IndexOutOfBoundsException("pos: " + pos);
            }
            CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
            requests[pos] = request;
            futures[pos] = future;
            pos++;
            return future;
        }

        public int size()
        {
            return size;
        }

        public Request[] getRequests()
        {
            return requests;
        }

        public CompletableFuture<ByteBuffer>[] getFutures()
        {
            return futures;
        }
    }
}
