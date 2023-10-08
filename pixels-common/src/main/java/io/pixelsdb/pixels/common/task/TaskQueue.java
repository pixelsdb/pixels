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
package io.pixelsdb.pixels.common.task;

import com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2023-07-26
 */
public class TaskQueue<E extends Task>
{
    private final LinkedList<E> allTasks;
    private final ConcurrentLinkedQueue<E> pendingQueue;
    private final ConcurrentHashMap<Integer, E> runningTasks;

    public TaskQueue()
    {
        this.allTasks = new LinkedList<>();
        this.pendingQueue = new ConcurrentLinkedQueue<>();
        this.runningTasks = new ConcurrentHashMap<>();
    }

    public TaskQueue(Collection<E> tasks)
    {
        checkPendingTasks(tasks);
        this.allTasks = new LinkedList<>(tasks);
        this.pendingQueue = new ConcurrentLinkedQueue<>(tasks);
        this.runningTasks = new ConcurrentHashMap<>();
    }

    /**
     * Load a batch of pending tasks into this task queue.
     * @param tasks the pending tasks
     * @return true if loaded successfully
     */
    public boolean offerAllPending(Collection<E> tasks)
    {
        checkPendingTasks(tasks);
        return this.pendingQueue.addAll(tasks);
    }

    private void checkPendingTasks(Collection<E> tasks)
    {
        checkArgument(tasks != null && !tasks.isEmpty(), "tasks should not be null or empty");
        for (E task : tasks)
        {
            checkArgument(task.getStatus() == Task.Status.PENDING,
                    "one of the tasks is not in pending status");
        }
    }

    /**
     * Load a pending task into this task queue.
     * @param task the pending task
     * @return true if loaded successfully
     */
    public boolean offerPending(E task)
    {
        return this.pendingQueue.offer(task);
    }

    /**
     * Poll one pending task from the head of the pending queue, set it as running,
     * and put it into the list of running tasks. This method is non-blocking.
     * @param worker the worker who is responsible for running the task
     * @return the task that is started and with a lease hold by the lease hold, or null if not such task
     */
    public E pollPendingAndRun(Worker<? extends WorkerInfo> worker)
    {
        E task = this.pendingQueue.poll();
        if (task != null)
        {
            task.start(worker);
            this.runningTasks.put(task.getTaskId(), task);
            return task;
        }
        else
        {
            return null;
        }
    }

    public boolean hasPending()
    {
        return !this.pendingQueue.isEmpty();
    }

    /**
     * Retrieve a running task and set its status to complete.
     * @param taskId the task id
     * @param success true if the task completes successfully
     * @return the task that is completed, or null if no such task
     */
    public E complete(int taskId, boolean success)
    {
        E task = this.runningTasks.remove(taskId);
        if (task != null)
        {
            task.complete(success);
            return task;
        }
        else
        {
            return null;
        }
    }

    /**
     * Iterate the list of running tasks, find and remove the next task with an expired lease.
     * @return the task that is found and remove, or null if no such task
     */
    public E removeNextExpired()
    {
        Iterator<Map.Entry<Integer, E>> iterator = this.runningTasks.entrySet().iterator();
        while (iterator.hasNext())
        {
            E task = iterator.next().getValue();
            if (!task.isRunningWell())
            {
                iterator.remove();
                return task;
            }
        }
        return null;
    }

    public List<E> getAllTasks()
    {
        return ImmutableList.copyOf(this.allTasks);
    }
}
