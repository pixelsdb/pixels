/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core.utils;

/**
 * dynamic int array from apache orc project
 * <p>
 * Dynamic int array that uses primitive types and chunks to avoid copying
 * large number of integers when it resizes.
 * <p>
 * The motivation for this class is memory optimization, i.e. space efficient
 * storage of potentially huge arrays without good a-priori size guesses.
 * <p>
 * The API of this class is between a primitive array and a AbstractList. It's
 * not a Collection implementation because it handles primitive types, but the
 * API could be extended to support iterators and the like.
 * <p>
 * NOTE: Like standard Collection implementations/arrays, this class is not
 * synchronized.
 */
public final class DynamicIntArray
{
    static final int DEFAULT_CHUNKSIZE = 8 * 1024;
    static final int INIT_CHUNKS = 256;

    private final int chunkSize;       // our allocation size
    private int[][] data;              // the real data
    private int length;                // max set element index +1
    private int initializedChunks = 0; // the number of created chunks

    public DynamicIntArray()
    {
        this(DEFAULT_CHUNKSIZE);
    }

    public DynamicIntArray(int chunkSize)
    {
        this.chunkSize = chunkSize;

        data = new int[INIT_CHUNKS][];
    }

    /**
     * Ensure that the given index is valid.
     */
    private void grow(int chunkIndex)
    {
        if (chunkIndex >= initializedChunks)
        {
            if (chunkIndex >= data.length)
            {
                int newSize = Math.max(chunkIndex + 1, 2 * data.length);
                int[][] newChunk = new int[newSize][];
                System.arraycopy(data, 0, newChunk, 0, data.length);
                data = newChunk;
            }
            for (int i = initializedChunks; i <= chunkIndex; ++i)
            {
                data[i] = new int[chunkSize];
            }
            initializedChunks = chunkIndex + 1;
        }
    }

    public int get(int index)
    {
        if (index >= length)
        {
            throw new IndexOutOfBoundsException("Index " + index +
                    " is outside of 0.." +
                    (length - 1));
        }
        int i = index / chunkSize;
        int j = index % chunkSize;
        return data[i][j];
    }

    public void set(int index, int value)
    {
        int i = index / chunkSize;
        int j = index % chunkSize;
        grow(i);
        if (index >= length)
        {
            length = index + 1;
        }
        data[i][j] = value;
    }

    public void increment(int index, int value)
    {
        int i = index / chunkSize;
        int j = index % chunkSize;
        grow(i);
        if (index >= length)
        {
            length = index + 1;
        }
        data[i][j] += value;
    }

    public void add(int value)
    {
        int i = length / chunkSize;
        int j = length % chunkSize;
        grow(i);
        data[i][j] = value;
        length += 1;
    }

    public int size()
    {
        return length;
    }

    public void clear()
    {
        length = 0;
        for (int i = 0; i < data.length; ++i)
        {
            data[i] = null;
        }
        initializedChunks = 0;
    }

    @Override
    public String toString()
    {
        int i;
        StringBuilder sb = new StringBuilder(length * 4);

        sb.append('{');
        int l = length - 1;
        for (i = 0; i < l; i++)
        {
            sb.append(get(i));
            sb.append(',');
        }
        sb.append(get(i));
        sb.append('}');

        return sb.toString();
    }

    /**
     * Convert this to an integer array. The returned array's length
     * is >= this.length, so that *DO NOT* use the length of the returned array.
     * If there are only one chunk used, no memory copy is performed.
     * @return
     */
    public int[] toArray()
    {
        if (initializedChunks == 1)
        {
            return data[0];
        }
        else
        {
            int[] array = new int[length];
            int i;
            for (i = 0; i < initializedChunks-1; i++)
            {
                System.arraycopy(data[i], 0, array, i*chunkSize, chunkSize);
            }
            int tail = length % chunkSize;
            for (int j = 0, k = i*chunkSize; j < tail; ++j, ++k)
            {
                array[k] = data[i][j];
            }
            return array;
        }
    }
}
