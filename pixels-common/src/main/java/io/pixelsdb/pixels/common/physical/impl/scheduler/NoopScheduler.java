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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Noop scheduler submits all the read requests in the batch one by one
 * in a FIFO manner.
 * If the reader supports asynchronous read, noop scheduler will do asynchronous read.
 * Request sort and request merge are not implemented in noop.
 *
 * Created at: 9/10/21
 * Author: hank
 */
public class NoopScheduler implements Scheduler
{
    private static Logger logger = LogManager.getLogger(NoopScheduler.class);

    @Override
    public CompletableFuture<Void> executeBatch(PhysicalReader reader, RequestBatch batch) throws IOException
    {
        CompletableFuture<ByteBuffer>[] futures = batch.getFutures();
        Request[] requests = batch.getRequests();
        if (reader.supportsAsync())
        {
            for (int i = 0; i < batch.size(); ++i)
            {
                CompletableFuture<ByteBuffer> future = futures[i];
                Request request = requests[i];
                reader.seek(request.start);
                reader.readAsync(request.length).whenComplete((resp, err) ->
                {
                    if (resp != null)
                    {
                        future.complete(resp);
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
            for (int i = 0; i < batch.size(); ++i)
            {
                Request request = requests[i];
                reader.seek(request.start);
                futures[i].complete(reader.readFully(request.length));
            }
        }

        return CompletableFuture.allOf(futures);
    }
}
