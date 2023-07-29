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
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author hank
 * @create 2023-07-26
 */
public class TaskQueue<E extends Task<?>>
{
    private final ConcurrentLinkedQueue<E> backingQueue;

    public TaskQueue()
    {
        this.backingQueue = new ConcurrentLinkedQueue<>();
    }

    public TaskQueue(Collection<E> tasks)
    {
        this.backingQueue = new ConcurrentLinkedQueue<>(tasks);
    }

    public boolean addAll(Collection<E> tasks)
    {
        return this.backingQueue.addAll(tasks);
    }

    public void clear()
    {
        this.backingQueue.clear();
    }

    public boolean offer(E task)
    {
        return this.backingQueue.offer(task);
    }

    public E poll()
    {
        return this.backingQueue.poll();
    }

    public E peek()
    {
        return this.backingQueue.peek();
    }
}
