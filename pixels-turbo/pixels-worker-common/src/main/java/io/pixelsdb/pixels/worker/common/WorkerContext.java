/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.worker.common;

import org.apache.logging.log4j.Logger;

/**
 * @author hank
 * @create 2023-04-22
 */
public class WorkerContext
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;
    private final String requestId;
    private final WorkerCancellation cancellation;
    private long memoryBytes;

    public WorkerContext(Logger logger, WorkerMetrics workerMetrics, String requestId)
    {
        this(logger, workerMetrics, requestId, WorkerCancellation.disabled());
    }

    private WorkerContext(Logger logger, WorkerMetrics workerMetrics, String requestId, WorkerCancellation cancellation)
    {
        this.logger = logger;
        this.workerMetrics = workerMetrics;
        this.requestId = requestId;
        this.cancellation = cancellation;
    }

    public WorkerContext withCancellation(WorkerCancellation cancellation)
    {
        WorkerContext context = new WorkerContext(logger, workerMetrics, requestId, cancellation);
        context.memoryBytes = memoryBytes;
        return context;
    }

    /** Actual platform memory, when the wrapper can report it (e.g. Lambda Context). */
    public WorkerContext withMemoryMB(int memoryMB)
    {
        WorkerContext context = withCancellation(cancellation);
        context.memoryBytes = Math.multiplyExact((long) memoryMB, 1024L * 1024);
        return context;
    }

    public long getMemoryBytes() { return memoryBytes; }

    public WorkerCancellation getCancellation() { return cancellation; }

    java.util.concurrent.ExecutorService createThreadPool(int threads, WorkerThreadExceptionHandler handler)
    {
        java.util.concurrent.ExecutorService executor = new WorkerThreadPool(threads, new WorkerThreadFactory(handler))
        {
            @Override
            protected void afterExecute(Runnable task, Throwable failure)
            {
                // Publish an execute() task's failure before the pool can
                // signal termination. JVM uncaught handlers run too late.
                if (failure != null) { handler.uncaughtException(Thread.currentThread(), failure); }
                super.afterExecute(task, failure);
            }

            @Override
            protected <T> java.util.concurrent.RunnableFuture<T> newTaskFor(java.util.concurrent.Callable<T> callable)
            {
                return new java.util.concurrent.FutureTask<T>(callable)
                {
                    @Override
                    public T get() throws InterruptedException, java.util.concurrent.ExecutionException
                    {
                        while (true)
                        {
                            cancellation.check();
                            try { return super.get(100, java.util.concurrent.TimeUnit.MILLISECONDS); }
                            catch (java.util.concurrent.TimeoutException ignored) { /* Check cancellation again. */ }
                        }
                    }
                };
            }

            @Override
            protected <T> java.util.concurrent.RunnableFuture<T> newTaskFor(Runnable runnable, T result)
            {
                return newTaskFor(java.util.concurrent.Executors.callable(runnable, result));
            }
        };
        cancellation.own(executor);
        return executor;
    }

    public Logger getLogger()
    {
        return logger;
    }

    public WorkerMetrics getWorkerMetrics()
    {
        return workerMetrics;
    }

    public String getRequestId()
    {
        return requestId;
    }
}
