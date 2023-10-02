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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2023-09-22
 */
public class StageCoordinator
{
    private static final Logger log = LogManager.getLogger(StageCoordinator.class);

    private final int stageId;
    private final boolean isQueued;
    private final int fixedWorkerNum;
    private final TaskQueue<Task<? extends Input>> taskQueue;
    private final Map<Long, Worker<CFWorkerInfo>> workerIdToWorkers = new ConcurrentHashMap<>();
    // this.workers is used for dependency checking, no concurrent reads and writes
    private final List<Worker<CFWorkerInfo>> workers = new ArrayList<>();
    private final Map<Long, Integer> workerIdToWorkerIndex = new ConcurrentHashMap<>();
    private final AtomicInteger workerIndex = new AtomicInteger(0);
    private final Object lock = new Object();

    public StageCoordinator(int stageId, int workerNum)
    {
        this.stageId = stageId;
        this.isQueued = false;
        this.fixedWorkerNum = workerNum;
        this.taskQueue = null;
    }

    public StageCoordinator(int stageId, List<Task<? extends Input>> pendingTasks)
    {
        this.stageId = stageId;
        this.isQueued = true;
        this.fixedWorkerNum = 0;
        this.taskQueue = new TaskQueue<>(pendingTasks);
    }

    public void addWorker(Worker<CFWorkerInfo> worker)
    {
        this.workerIdToWorkers.put(worker.getWorkerId(), worker);
        this.workerIdToWorkerIndex.put(worker.getWorkerId(), this.workerIndex.getAndIncrement());
        if (!this.isQueued && this.workers.size() == this.fixedWorkerNum)
        {
            this.lock.notifyAll();
        }
    }

    public Task<? extends Input> getTaskToRun(long workerId)
    {
        checkArgument(this.isQueued && this.taskQueue != null,
                "can not get task to run on a non-queued stage");
        Worker<CFWorkerInfo> worker = this.workerIdToWorkers.get(workerId);
        if (worker != null)
        {
            Task<? extends Input> task = this.taskQueue.pollPendingAndRun(worker);
            if (task == null || this.taskQueue.hasPending())
            {
                this.lock.notifyAll();
            }
            return task;
        }
        else
        {
            return null;
        }
    }

    public int getStageId()
    {
        return this.stageId;
    }

    public Worker<CFWorkerInfo> getWorker(long workerId)
    {
        return this.workerIdToWorkers.get(workerId);
    }

    /**
     * Get the index of the worker in this stage, the index starts from 0.
     * @param workerId the (global) id of the worker
     * @return the index of the worker in this stage, or < 0 if the worker is not found
     */
    public int getWorkerIndex(long workerId)
    {
        Integer index = this.workerIdToWorkerIndex.get(workerId);
        if (index != null)
        {
            return index;
        }
        return -1;
    }

    void waitForAllWorkersReady()
    {
        synchronized (this.lock)
        {
            if (this.isQueued && this.taskQueue != null)
            {
                while (this.taskQueue.hasPending())
                {
                    try
                    {
                        this.lock.wait();
                    } catch (InterruptedException e)
                    {
                        log.error("interrupted while waiting for the pending tasks to be executed");
                    }
                }
            }
            else
            {
                while (this.workers.size() < this.fixedWorkerNum)
                {
                    try
                    {
                        this.lock.wait();
                    } catch (InterruptedException e)
                    {
                        log.error("interrupted while waiting workers to arrive");
                    }
                }
            }
        }
    }

    public List<Worker<CFWorkerInfo>> getWorkers()
    {
        return this.workers;
    }
}
