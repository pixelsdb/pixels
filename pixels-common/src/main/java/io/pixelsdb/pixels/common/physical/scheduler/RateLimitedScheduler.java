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

import com.google.common.util.concurrent.RateLimiter;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Scheduler;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

/**
 * <p>
 *     RateLimitedScheduler adds the data transfer rate limit and the
 *     request throughput limit to SortMergeScheduler.
 * </p>
 * <p>
 *     Issue #142:
 *     We future confirm that rate limits on both mbps and rps help keep large query
 *     performance stable.
 * </p>
 *
 * @author hank
 * @create 2021-10-16
 */
public class RateLimitedScheduler extends SortMergeScheduler
{
    private static Logger logger = LogManager.getLogger(RateLimitedScheduler.class);
    private static RateLimitedScheduler instance;

    public static Scheduler Instance()
    {
        if (instance == null)
        {
            instance = new RateLimitedScheduler();
        }
        return instance;
    }

    private RateLimiter mbpsRateLimiter;
    private RateLimiter rpsRateLimiter;
    private final Random random;

    protected RateLimitedScheduler()
    {
        ConfigFactory.Instance().registerUpdateCallback("read.request.rate.limit.mbps", value ->
        {
            double mbpsRateLimit = Double.parseDouble(value);
            if (mbpsRateLimiter == null)
            {
                mbpsRateLimiter = RateLimiter.create(mbpsRateLimit);
            }
            else
            {
                mbpsRateLimiter.setRate(mbpsRateLimit);
            }
        });
        double mbpsRateLimit = Double.parseDouble(ConfigFactory.Instance().getProperty("read.request.rate.limit.mbps"));
        mbpsRateLimiter = RateLimiter.create(mbpsRateLimit);

        ConfigFactory.Instance().registerUpdateCallback("read.request.rate.limit.rps", value ->
        {
            double rpsRateLimit = Double.parseDouble(value);
            if (rpsRateLimiter == null)
            {
                rpsRateLimiter = RateLimiter.create(rpsRateLimit);
            }
            else
            {
                rpsRateLimiter.setRate(rpsRateLimit);
            }
        });
        double rpsRateLimit = Double.parseDouble(ConfigFactory.Instance().getProperty("read.request.rate.limit.rps"));
        rpsRateLimiter = RateLimiter.create(rpsRateLimit);

        random = new Random(System.nanoTime());
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
            Storage.Scheme scheme = reader.getStorageScheme();
            if (scheme == Storage.Scheme.s3)
            {
                // Only do rate limit for S3 async reads.
                long bytes = 0;
                for (MergedRequest merged : mergedRequests)
                {
                    bytes += merged.getLength();
                }
                double mb = (bytes/1024.0/1024.0);
                int mbLimit = (int) mb;
                // Issue #142: ensure the accuracy using random.
                mbLimit += random.nextDouble() <= (mb - mbLimit) ? 1 : 0;
                if (mbLimit > 0)
                {
                    mbpsRateLimiter.acquire(mbLimit);
                }
                rpsRateLimiter.acquire(mergedRequests.size());
            }

            for (MergedRequest merged : mergedRequests)
            {
                String path = reader.getPath();
                merged.startTimeMs = System.currentTimeMillis();
                reader.readAsync(merged.getStart(), merged.getLength()).thenAccept(resp ->
                {
                    if (resp != null)
                    {
                        merged.completeTimeMs = System.currentTimeMillis();
                        merged.complete(resp);
                    }
                    else
                    {
                        logger.error("Asynchronous read from path '" + path + "' got null response, start=" +
                                merged.getStart() + ", length=" + merged.getLength());
                    }
                });
                if (enableRetry)
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
}
