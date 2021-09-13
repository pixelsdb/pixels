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
package io.pixelsdb.pixels.common.physical.impl.scheduler;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Scheduler;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * SortMerge scheduler firstly sorts the requests in the batch by the start offset,
 * then it tries to merge the requests that can be read sequentially from the reader.
 * Created at: 9/12/21
 * Author: hank
 */
public class SortMergeScheduler implements Scheduler
{
    private static Logger logger = LogManager.getLogger(SortMergeScheduler.class);
    private static int MaxGap;

    static
    {
        ConfigFactory.Instance().registerUpdateCallback("read.request.merge.gap", value ->
                MaxGap = Integer.parseInt(ConfigFactory.Instance().getProperty("read.request.merge.gap")));
        MaxGap = Integer.parseInt(ConfigFactory.Instance().getProperty("read.request.merge.gap"));
    }

    @Override
    public CompletableFuture<Void> executeBatch(PhysicalReader reader, RequestBatch batch) throws IOException
    {
        if (batch.size() <= 0)
        {
            return batch.completeAll();
        }
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

        if (reader.supportsAsync())
        {
            for (MergedRequest merged : mergedRequests)
            {
                reader.seek(merged.getStart());
                reader.readAsync(merged.getLength()).whenComplete((resp, err) ->
                {
                    if (resp != null)
                    {
                        merged.complete(resp);
                    }
                    else
                    {
                        logger.error("Failed to read asynchronously from path '" +
                                reader.getPath() + "'.", err);
                        err.printStackTrace();
                    }
                });
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

        return batch.completeAll();
    }

    class RequestFuture implements Comparable<RequestFuture>
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

    class MergedRequest
    {
        private long start;
        private long end;
        private int position;
        private int size;
        private List<Integer> positions;
        private List<Integer> lengths;
        private List<CompletableFuture<ByteBuffer>> futures;

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

        public int getLength()
        {
            return (int) (end - start);
        }

        public int getSize()
        {
            return size;
        }

        /**
         * When the data has been read, complete all the
         * futures,
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
}
