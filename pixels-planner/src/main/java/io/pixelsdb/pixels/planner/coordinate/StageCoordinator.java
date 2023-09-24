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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hank
 * @create 2023-09-22
 */
public class StageCoordinator
{
    private final int stageId;
    private final TaskQueue<Task<? extends Input>> taskQueue;
    private final Map<Long, Worker<CFWorkerInfo>> workerIdToWorkers;
    private final List<Worker<CFWorkerInfo>> workers;

    public StageCoordinator(int stageId)
    {
        this.stageId = stageId;
        this.taskQueue = new TaskQueue<>();
        this.workerIdToWorkers = new ConcurrentHashMap<>();
        this.workers = new ArrayList<>(); // used for dependency checking, no concurrent reads and writes
    }

    public StageCoordinator(int stageId, List<Task<? extends Input>> tasks)
    {
        this.stageId = stageId;
        this.taskQueue = new TaskQueue<>(tasks);
        this.workerIdToWorkers = new ConcurrentHashMap<>();
        this.workers = new ArrayList<>(); // used for dependency checking, no concurrent reads and writes
    }

    public void addPendingTask(Task<? extends Input> task)
    {
        this.taskQueue.offerPending(task);
    }

    public void addWorker(Worker<CFWorkerInfo> worker)
    {
        this.workerIdToWorkers.put(worker.getWorkerId(), worker);
    }

    public int getStageId()
    {
        return stageId;
    }

    public Worker<CFWorkerInfo> getWorker(long workerId)
    {
        return this.workerIdToWorkers.get(workerId);
    }

    public List<Worker<CFWorkerInfo>> getWorkers()
    {
        return this.workers;
    }
}
