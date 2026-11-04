/*
 * Copyright 2026 PixelsDB.
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
