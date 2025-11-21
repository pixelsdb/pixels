/*
 * Copyright 2022 PixelsDB.
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

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2022-08-11
 */
public class RetryPolicy
{
    private static final Logger logger = LogManager.getLogger(RetryPolicy.class);
    private final int maxRetryNum;
    private final int intervalMs;
    private final ConcurrentLinkedQueue<ExecutableRequest> requests;
    private ExecutorService monitorService;
    private volatile boolean stopped = false;

    private static final int FIRST_BYTE_LATENCY_MS = 1000; // 1000ms
    private static final int TRANSFER_RATE_BPMS = 4096; // 4KB/ms, i.e., 4MB/s

    /**
     * Create a retry policy, which is running as a daemon thread, for retrying the timed out requests.
     * The policy will periodically check the request queue and find requests to retry.
     * @param intervalMs the interval in milliseconds of two subsequent request queue checks.
     */
    public RetryPolicy(int intervalMs)
    {
        this.maxRetryNum = Integer.parseInt(ConfigFactory.Instance().getProperty("read.request.max.retry.num"));
        this.intervalMs = intervalMs;
        this.requests = new ConcurrentLinkedQueue<>();
        this.start();
        ShutdownHookManager.Instance().registerShutdownHook(RetryPolicy.class, true, this::stop);
    }

    private void start()
    {
        // Issue #133: set the monitor thread as daemon thread with max priority.
        ThreadGroup monitorThreadGroup = new ThreadGroup("pixels.retry.monitor");
        monitorThreadGroup.setMaxPriority(Thread.MAX_PRIORITY);
        monitorThreadGroup.setDaemon(true);
        this.monitorService = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(monitorThreadGroup, runnable);
            thread.setDaemon(true);
            thread.setPriority(Thread.MAX_PRIORITY);
            return thread;
        });

        this.monitorService.execute(() ->
        {
            while (!stopped)
            {
                long currentTimeMs = System.currentTimeMillis();
                for (Iterator<ExecutableRequest> it = requests.iterator(); it.hasNext(); )
                {
                    ExecutableRequest request = it.next();
                    if (request.getCompleteTimeMs() > 0)
                    {
                        // request has completed.
                        it.remove();
                    } else if (currentTimeMs - request.getStartTimeMs() > timeoutMs(request.getLength()))
                    {
                        if (request.getRetried() >= maxRetryNum)
                        {
                            // give up retrying.
                            it.remove();
                            continue;
                        }
                        // retry request.
                        if (!request.execute())
                        {
                            // no need to continue retrying
                            it.remove();
                        }
                    }
                }
                try
                {
                    // sleep for some time, to release the cpu.
                    TimeUnit.MILLISECONDS.sleep(this.intervalMs);
                } catch (InterruptedException e)
                {
                    logger.error("Retry policy is interrupted, exiting", e);
                    break;
                }
            }
        });

        this.monitorService.shutdown();
    }

    private void stop()
    {
        this.stopped = true;

        try
        {
            if (!this.monitorService.awaitTermination(10, TimeUnit.SECONDS))
            {
                logger.info("retry policy did not terminate within 10 seconds, force terminate it");
                this.monitorService.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            logger.error("retry policy is interrupted during stopping", e);
        }
    }

    private int timeoutMs(int length)
    {
        return FIRST_BYTE_LATENCY_MS + length / TRANSFER_RATE_BPMS;
    }

    public void monitor(ExecutableRequest request)
    {
        this.requests.add(request);
    }

    public interface ExecutableRequest
    {
        long getStartTimeMs();

        long getCompleteTimeMs();

        int getLength();

        int getRetried();

        /**
         * Try to execute this request.
         * @return true if retry monitoring should continue, false if no need to retry
         */
        boolean execute();
    }
}
