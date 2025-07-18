/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.cache;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * memory efficient dynamic array
 * <p>
 * the dynamic array will resize itself to fit its real usage.
 * when the array is full, upon new add() operations, the array will grow to its capacity
 * according to its growth factor(default value is 2x) automatically.
 * when the array is too sparse to meet the utilization payload(default value is 0.6),
 * the array will shrink its capacity automatically.
 *
 * @author guodong
 * @author hank
 */

@NotThreadSafe
public class DynamicArray<T>
{
    private static final int DEFAULT_CHUNK_SIZE = 1024;
    private static final int DEFAULT_CHUNK_NUM = 1;
    private static final int DEFAULT_GROWTH_FACTOR = 2;
    private static final float DEFAULT_UTILIZATION_PAYLOAD = 0.6f;
    private final int chunkSize;
    private final int growthFactor;
    private final float utilizationPayload;

    private int initializedChunkNum = 0;
    private int chunkNum;
    private float utilization = 0.0f;

    private int size = 0;                             // current number of elements in the array

    private Object[][] content;

    public DynamicArray()
    {
        this(DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_NUM, DEFAULT_GROWTH_FACTOR, DEFAULT_UTILIZATION_PAYLOAD);
    }

    public DynamicArray(int initChunkSize)
    {
        this(initChunkSize, DEFAULT_CHUNK_NUM, DEFAULT_GROWTH_FACTOR, DEFAULT_UTILIZATION_PAYLOAD);
    }

    public DynamicArray(int initChunkSize, int initChunkNum)
    {
        this(initChunkSize, initChunkNum, DEFAULT_GROWTH_FACTOR, DEFAULT_UTILIZATION_PAYLOAD);
    }

    public DynamicArray(int initChunkSize, int initChunkNum, int growthFactor, float utilizationPayload)
    {
        this.chunkSize = initChunkSize;
        this.chunkNum = initChunkNum;
        this.growthFactor = growthFactor;
        this.utilizationPayload = utilizationPayload;
        this.content = new Object[chunkNum][];
    }

    /**
     * Grow the array according to specified growth factor.
     * If growth factor is 2, then capacity of array will be double of its original one after growth.
     */
    private void grow(int chunkIndex)
    {
        if (chunkIndex >= chunkNum)
        {
            // grow content length
            int newChunkNum = chunkNum * growthFactor;
            Object[][] newContent = new Object[newChunkNum][];
            System.arraycopy(content, 0, newContent, 0, chunkNum);
            content = newContent;
            chunkNum = newChunkNum;
        }
        // allocate new chunk
        for (int i = initializedChunkNum; i <= chunkIndex; i++)
        {
            content[i] = new Object[chunkSize];
        }
        initializedChunkNum = chunkIndex + 1;
    }

    /**
     * Compact the array to avoid useless memory occupation. During compaction, useless chunks are removed.
     * Compaction is triggered when utilization is lower than utilization payload.
     */
    private void compact()
    {
    }

    /**
     * Add an element.
     */
    public int add(T v)
    {
        int index = size;
        int chunkIndex = index / chunkSize;
        int chunkOffset = index % chunkSize;
        // check capacity
        if (index >= chunkSize * initializedChunkNum)
        {
            grow(chunkIndex);
        }
        // add element
        content[chunkIndex][chunkOffset] = v;
        size++;

        return index;
    }

    /**
     * Set value of the specified index.
     *
     * @throws ArrayIndexOutOfBoundsException array index out of bounds
     */
    public void set(int index, T v)
    {
        if (index >= size)
        {
            throw new ArrayIndexOutOfBoundsException("array index " + index + " is out of bound");
        }
        int chunkIndex = index / chunkSize;
        int chunkOffset = index % chunkSize;
        content[chunkIndex][chunkOffset] = v;
    }

    /**
     * Get value of the specified index.
     *
     * @throws ArrayIndexOutOfBoundsException array index out of bounds
     */
    @SuppressWarnings("unchecked")
    public T get(int index)
    {
        if (index >= size)
        {
            throw new ArrayIndexOutOfBoundsException("array index " + index + " is out of bound");
        }
        int chunkIndex = index / chunkSize;
        int chunkOffset = index % chunkSize;
        return (T) content[chunkIndex][chunkOffset];
    }

    /**
     * Remove value of the specified index.
     */
    public void remove(int index)
    {
    }

    public void removeAll ()
    {
        this.size = 0;
        this.chunkNum = DEFAULT_CHUNK_NUM;
        this.content = new Object[chunkNum][];
        this.initializedChunkNum = 0;
    }

    /**
     * Release reference of specified index.
     * After release, the object may be cleared to let GC do its work.
     *
     * @throws ArrayIndexOutOfBoundsException array index out of bounds
     */
    public void release(int index)
    {
        if (index >= size)
        {
            throw new ArrayIndexOutOfBoundsException("array index " + index + " is out of bound");
        }
        int chunkIndex = index / chunkSize;
        int chunkOffset = index % chunkSize;
        content[chunkIndex][chunkOffset] = null;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int size()
    {
        return this.size;
    }

    public int capacity()
    {
        return chunkNum * chunkSize;
    }

    public int growthFactor()
    {
        return this.growthFactor;
    }

    public float utilizationPayload()
    {
        return this.utilizationPayload;
    }
}
