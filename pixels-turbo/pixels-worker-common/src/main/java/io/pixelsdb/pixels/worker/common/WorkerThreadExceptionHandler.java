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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hank
 * @create 2023-07-31
 */
public class WorkerThreadExceptionHandler implements Thread.UncaughtExceptionHandler
{
    private final Logger logger;
    private final AtomicBoolean hasException;
    private final WorkerCancellation cancellation;
    private final java.util.concurrent.atomic.AtomicReference<Throwable> firstFailure = new java.util.concurrent.atomic.AtomicReference<>();

    public WorkerThreadExceptionHandler(Logger logger)
    {
        this(logger, WorkerCancellation.disabled());
    }

    WorkerThreadExceptionHandler(Logger logger, WorkerCancellation cancellation)
    {
        this.logger = logger;
        this.hasException = new AtomicBoolean(false);
        this.cancellation = cancellation;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e)
    {
        if (firstFailure.compareAndSet(null, e))
        {
            hasException.set(true);
            cancellation.cancel(e);
            logger.error(String.format("error occurred in thread: %s", t.getName()), e);
        }
    }

    public Throwable getException() { return firstFailure.get(); }

    public boolean hasException()
    {
        return hasException.get();
    }
}
