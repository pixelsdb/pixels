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

import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.task.TaskQueue;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.turbo.Input;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hank
 * @create 2023-09-22
 */
public class StageCoordinator
{
    private final TaskQueue<Task<? extends Input>> taskQueue;
    private final Map<Long, Worker<CFWorkerInfo>> workers;

    public StageCoordinator()
    {
        this.taskQueue = new TaskQueue<>();
        this.workers = new ConcurrentHashMap<>();
    }

    public StageCoordinator(List<Task<? extends Input>> tasks)
    {
        this.taskQueue = new TaskQueue<>(tasks);
        this.workers = new ConcurrentHashMap<>();
    }

    public void addPendingTask(Task<? extends Input> task)
    {
        this.taskQueue.offerPending(task);
    }

    public void addWorker(long workerId, Worker<CFWorkerInfo> worker)
    {
        this.workers.put(workerId, worker);
    }
}
