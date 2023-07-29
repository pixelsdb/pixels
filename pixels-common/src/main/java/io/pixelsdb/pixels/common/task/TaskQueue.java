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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author hank
 * @create 2023-07-26
 */
public class TaskQueue<E extends Task<?>>
{
    private final ConcurrentLinkedQueue<E> pendingQueue;
    private final ConcurrentHashMap<String, E> runningTasks;

    public TaskQueue()
    {
        this.pendingQueue = new ConcurrentLinkedQueue<>();
        this.runningTasks = new ConcurrentHashMap<>();
    }

    public TaskQueue(Collection<E> tasks)
    {
        this.pendingQueue = new ConcurrentLinkedQueue<>(tasks);
        this.runningTasks = new ConcurrentHashMap<>();
    }

    public boolean offerAllPending(Collection<E> tasks)
    {
        return this.pendingQueue.addAll(tasks);
    }

    public boolean offerPending(E task)
    {
        return this.pendingQueue.offer(task);
    }

    public E pollPendingAndRun(LeaseHolder leaseHolder)
    {
        E task = this.pendingQueue.poll();
        if (task != null)
        {
            task.start(leaseHolder);
            this.runningTasks.put(task.getId(), task);
            return task;
        }
        else
        {
            return null;
        }
    }

    public E complete(String taskId, LeaseHolder leaseHolder)
    {
        E task = this.runningTasks.remove(taskId);
        if (task != null)
        {
            task.complete(leaseHolder);
            return task;
        }
        else
        {
            return null;
        }
    }

    public E removeNextExpired()
    {
        long currentTimeMs = System.currentTimeMillis();
        Iterator<E> iterator = this.pendingQueue.iterator();
        while (iterator.hasNext())
        {
            E task = iterator.next();
            if (task.getLease().hasExpired(currentTimeMs))
            {
                iterator.remove();
                return task;
            }
        }
        return null;
    }
}
