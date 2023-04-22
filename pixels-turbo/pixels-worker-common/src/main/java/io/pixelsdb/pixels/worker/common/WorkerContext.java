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

import org.slf4j.Logger;

/**
 * @author hank
 * @create 2023-04-22
 */
public class WorkerContext
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;
    private final String requestId;

    public WorkerContext(Logger logger, WorkerMetrics workerMetrics, String requestId)
    {
        this.logger = logger;
        this.workerMetrics = workerMetrics;
        this.requestId = requestId;
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
