/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Release task-owned resources only after the last child really exits. */
class WorkerThreadPool extends ThreadPoolExecutor
{
    private final CompletableFuture<Void> stopped = new CompletableFuture<>();

    WorkerThreadPool(int threads, ThreadFactory factory)
    {
        super(threads, threads, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), factory);
    }

    void afterTermination(Runnable cleanup) { stopped.thenRun(cleanup); }

    @Override
    protected void terminated()
    {
        try { stopped.complete(null); }
        finally { super.terminated(); }
    }
}
