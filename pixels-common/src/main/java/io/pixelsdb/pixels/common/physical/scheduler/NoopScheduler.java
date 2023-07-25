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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Noop scheduler submits all the read requests in the batch one by one
 * in a FIFO manner.
 * If the reader supports asynchronous read, noop scheduler will do asynchronous read.
 * Request sort and request merge are not implemented in noop.
 *
 * @author hank
 * @create 2021-09-10
 */
public class NoopScheduler implements Scheduler
{
    private static Logger logger = LogManager.getLogger(NoopScheduler.class);
    private static NoopScheduler instance;

    public static Scheduler Instance()
    {
        if (instance == null)
        {
            instance = new NoopScheduler();
        }
        return instance;
    }

    protected NoopScheduler() {}

    @Override
    public void executeBatch(PhysicalReader reader, RequestBatch batch, long transId) throws IOException
    {
        if (batch.size() <= 0)
        {
            return;
        }
        List<CompletableFuture<ByteBuffer>> futures = batch.getFutures();
        List<Request> requests = batch.getRequests();
        if (reader.supportsAsync())
        {
            for (int i = 0; i < batch.size(); ++i)
            {
                CompletableFuture<ByteBuffer> future = futures.get(i);
                Request request = requests.get(i);
                String path = reader.getPath();
                reader.readAsync(request.start, request.length).thenAccept(resp ->
                {
                    if (resp != null)
                    {
                        future.complete(resp);
                    }
                    else
                    {
                        logger.error("Asynchronous read from path '" + path + "' got null response, start=" +
                                request.start + ", length=" + request.length);
                    }
                });
            }
        }
        else
        {
            for (int i = 0; i < batch.size(); ++i)
            {
                Request request = requests.get(i);
                reader.seek(request.start);
                futures.get(i).complete(reader.readFully(request.length));
            }
        }
    }
}
