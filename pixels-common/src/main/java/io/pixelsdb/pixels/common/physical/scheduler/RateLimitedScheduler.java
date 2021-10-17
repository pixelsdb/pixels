package io.pixelsdb.pixels.common.physical.scheduler;

import com.google.common.util.concurrent.RateLimiter;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created at: 16/10/2021
 * Author: hank
 */
public class RateLimitedScheduler extends SortMergeScheduler
{
    private static Logger logger = LogManager.getLogger(RateLimitedScheduler.class);

    private RateLimiter mbpsRateLimiter;
    private RateLimiter rpsRateLimiter;
    private RetryPolicy retryPolicy;

    RateLimitedScheduler()
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

        this.retryPolicy = new RetryPolicy(1000);
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
            Storage.Scheme scheme = reader.getStorageScheme();
            if (scheme == Storage.Scheme.s3)
            {
                // Only do rate limit for S3.
                long bytes = 0;
                for (MergedRequest merged : mergedRequests)
                {
                    bytes += merged.getLength();
                }
                mbpsRateLimiter.acquire((int) (bytes/1024.0/1024.0));
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
                        logger.error("Failed to read asynchronously from path '" +
                                path + "'.");
                    }
                });
                this.retryPolicy.add(merged, reader);
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
