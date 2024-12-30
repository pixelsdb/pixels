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

import io.pixelsdb.pixels.common.exception.WorkerCoordinateException;
import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.task.TaskQueue;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The coordinator of a query execution stage.
 * @author hank
 * @create 2023-09-22
 */
public class StageCoordinator
{
    private static final Logger log = LogManager.getLogger(StageCoordinator.class);
    private static final int WorkerTaskParallelism;

    static
    {
        WorkerTaskParallelism = Integer.parseInt(ConfigFactory.Instance()
                .getProperty("executor.intra.worker.parallelism"));
    }

    private final int stageId;
    private final boolean isQueued;
    private final int fixedWorkerNum;
    private final TaskQueue<Task> taskQueue;
    private final Map<Long, Worker<CFWorkerInfo>> workerIdToWorkers = new ConcurrentHashMap<>();
    // this.workers is used for dependency checking, no concurrent reads and writes
    private final List<Worker<CFWorkerInfo>> workers = new ArrayList<>();
    private final Map<Long, Integer> workerIdToWorkerIndex = new ConcurrentHashMap<>();
    private final AtomicInteger workerIndexAssigner = new AtomicInteger(0);
    private final Object lock = new Object();

    /**
     * Create a non-queued stage coordinator with a fixed number of workers in this stage. Non-queued stage coordinator
     * is used when the upstream (child) stage has a wide dependency on this stage. The tasks in a non-queued stage
     * are send directly to the workers, thus the workers does not need to call {@link #getTasksToRun(long)} on this
     * stage coordinator. However, the workers still need to register by calling {@link #addWorker(Worker)} when they
     * are up and running. The stage coordinator will notify the child stage waits on {@link #waitForAllWorkersReady()}
     * when all the workers in this stage are registered (i.e., up and running). This is required by a wide dependency.
     * @param stageId the id of this stage
     * @param workerNum the fixed number of workers in this stage
     */
    public StageCoordinator(int stageId, int workerNum)
    {
        this.stageId = stageId;
        this.isQueued = false;
        this.fixedWorkerNum = workerNum;
        this.taskQueue = null;
    }

    /**
     * Create a queued stage coordinator with a list of pending tasks. Queued stage coordinator is used when the
     * upstream (child) stage does not exist or has a narrow dependency on this stage. The stage coordinator will
     * notify the child stage waits on {@link #waitForAllWorkersReady()} when there is none tasks pending in this
     * stage (i.e., when all the tasks are assigned to corresponding workers, meaning no more workers will be added).
     * @param stageId
     * @param pendingTasks
     */
    public StageCoordinator(int stageId, List<Task> pendingTasks)
    {
        this.stageId = stageId;
        this.isQueued = true;
        this.fixedWorkerNum = 0;
        this.taskQueue = new TaskQueue<>(pendingTasks);
    }

    /**
     * Add (register) a worker into this stage coordinator.
     * @param worker the worker to be added
     */
    public void addWorker(Worker<CFWorkerInfo> worker)
    {
        synchronized (this.lock)
        {
            this.workerIdToWorkers.put(worker.getWorkerId(), worker);
            this.workerIdToWorkerIndex.put(worker.getWorkerId(), this.workerIndexAssigner.getAndIncrement());
            this.workers.add(worker);
            if (!this.isQueued && this.workers.size() == this.fixedWorkerNum)
            {
                this.lock.notifyAll();
            }
        }
    }

    /**
     * Get a batch of tasks from the task queue to execute by a worker. This method should only be called on a
     * queued stage. The number of tasks in a batch usually equals to the task parallelism in a worker, which is
     * configured in pixels.properties.
     * @param workerId the id of the worker
     * @return the batch of tasks
     * @throws WorkerCoordinateException if the worker of the id does not exist
     */
    public List<Task> getTasksToRun(long workerId) throws WorkerCoordinateException
    {
        checkArgument(this.isQueued && this.taskQueue != null,
                "can not get task to run on a non-queued stage");
        Worker<CFWorkerInfo> worker = this.workerIdToWorkers.get(workerId);
        if (worker != null)
        {
            List<Task> tasks = new ArrayList<>(WorkerTaskParallelism);
            for (int i = 0; i < WorkerTaskParallelism; ++i)
            {
                Task task = this.taskQueue.pollPendingAndRun(worker);
                if (task == null)
                {
                    break;
                }
                tasks.add(task);
            }
            if (!this.taskQueue.hasPending())
            {
                synchronized (this.lock)
                {
                    this.lock.notifyAll();
                }
            }
            return tasks;
        }
        else
        {
            String msg = "worker of id " + workerId + "does not exist";
            log.error(msg);
            throw new WorkerCoordinateException(msg);
        }
    }

    /**
     * Complete a task on a queued stage. This method should not be called on non-queued stage.
     * @param taskId the task id
     * @param success whether the task is completed successfully or not
     */
    public void completeTask(int taskId, boolean success)
    {
        checkArgument(this.isQueued && this.taskQueue != null,
                "can not complete task on a non-queued stage");
        this.taskQueue.complete(taskId, success);
    }

    /**
     * @return the stage id of this stage
     */
    public int getStageId()
    {
        return this.stageId;
    }

    /**
     * Get the worker with the worker id.
     * @param workerId the worker id
     * @return the worker
     */
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

    /**
     * Block and wait for all the workers on this stage to ready.
     */
    public void waitForAllWorkersReady()
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
    // todo: should we write a "waitForWorkerReady(int workerId)" method in WorkerCoordinateServiceImpl
    //  for non-wide stages? Currently, we only have "waitForAllWorkersReady()" for wide stages.

    /**
     * @return all the workers in this stage coordinator
     */
    public List<Worker<CFWorkerInfo>> getWorkers()
    {
        return this.workers;
    }
}
