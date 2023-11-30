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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.task.Worker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hank
 * @create 2023-09-21
 */
public class CFWorkerManager
{
    private static final CFWorkerManager instance = new CFWorkerManager();

    public static CFWorkerManager Instance()
    {
        return instance;
    }

    private final Map<Long, Worker<CFWorkerInfo>> workerIdToWorker;
    private final AtomicLong workerId;

    private CFWorkerManager()
    {
        this.workerIdToWorker = new ConcurrentHashMap<>();
        this.workerId = new AtomicLong(0);
    }

    public long createWorkerId()
    {
        return workerId.getAndIncrement();
    }

    public void registerCFWorker(Worker<CFWorkerInfo> worker)
    {
        this.workerIdToWorker.put(worker.getWorkerId(), worker);
    }

    public Worker<CFWorkerInfo> getCFWorker(long workerId)
    {
        return this.workerIdToWorker.get(workerId);
    }
}
