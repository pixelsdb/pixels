/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.daemon.scaling.policy.helper;

import java.util.LinkedList;
import java.util.Queue;

/**
 * FixedSizeQueue is a fixed-capacity queue that automatically removes the head element
 * when a new element is added and the queue is full. It also provides a method to calculate
 * the average of the elements in the queue.
 *
 * <p>Thread-safety:
 * This class is not thread-safe. If multiple threads access an instance of FixedSizeQueue
 * concurrently, external synchronization is required.
 *
 * <p>Important considerations:
 * - This queue automatically removes the head element if the capacity is exceeded when adding
 *   new elements.
 * - It only supports numeric elements as it includes an average calculation method.
 *
 * @author zhujiaxuan
 * @create 2024-10-10
 */

public class FixedSizeQueue
{
    private final int maxSize;
    private final Queue<Integer> queue;
    private int sum;

    public FixedSizeQueue(int size)
    {
        assert(size > 0);
        this.maxSize = size;
        this.queue = new LinkedList<>();
        this.sum = 0;
    }

    /**
     * add a new element
     * @param value
     */
    public void add(int value)
    {
        if (queue.size() == maxSize)
        {
            int removed = queue.poll();
            sum -= removed;
        }
        queue.offer(value);
        sum += value;
    }

    /**
     * clear all the elements in the queue
     */
    public void clear() {
        queue.clear();
        sum = 0;
    }

    /**
     * @return the average of the elements in the queue
     */
    public double getAverage()
    {
        if (queue.isEmpty())
        {
            return 0.0;
        }
        return (double) sum / queue.size();
    }

    public String toString()
    {
        return queue.toString();
    }

}
