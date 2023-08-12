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

    public WorkerThreadExceptionHandler(Logger logger)
    {
        this.logger = logger;
        this.hasException = new AtomicBoolean(false);
    }

    @Override
    public void uncaughtException(Thread t, Throwable e)
    {
        logger.error(String.format("error occurred in thread: %s", t.getName()), e);
        hasException.set(true);
    }

    public boolean hasException()
    {
        return hasException.get();
    }
}
