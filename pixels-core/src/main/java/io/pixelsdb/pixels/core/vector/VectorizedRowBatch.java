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
package io.pixelsdb.pixels.core.vector;

/**
 * VectorizedRowBatch derived from org.apache.hadoop.hive.ql.exec.vector
 * <p>
 * A VectorizedRowBatch is a set of rows, organized with each column
 * as a vector. It is the unit of query execution, organized to minimize
 * the cost per row and achieve high cycles-per-instruction.
 * The major fields are public by design to allow fast and convenient
 * access by the vectorized query execution code.
 */
public class VectorizedRowBatch implements AutoCloseable
{
    public int numCols;           // number of columns
    public ColumnVector[] cols;   // a vector for each column
    public int size;              // number of rows that qualify (i.e. haven't been filtered out)
    public int projectionSize;
    public int maxSize;

    private long memoryUsage = 0L;

    // If this is true, then there is no data in the batch -- we have hit the end of input.
    public boolean endOfFile;

    /*
     * This number is carefully chosen to minimize overhead and typically allows
     * one VectorizedRowBatch to fit in cache.
     */
    public static final int DEFAULT_SIZE = 1024;

    /**
     * Return a batch with the specified number of columns.
     * This is the standard constructor -- all batches should be the same size
     *
     * @param numCols the number of columns to include in the batch
     */
    public VectorizedRowBatch(int numCols)
    {
        this(numCols, DEFAULT_SIZE);
    }

    /**
     * Return a batch with the specified number of columns and rows.
     * Only call this constructor directly for testing purposes.
     * Batch size should normally always be defaultSize.
     *
     * @param numCols the number of columns to include in the batch
     * @param size    the number of rows to include in the batch
     */
    public VectorizedRowBatch(int numCols, int size)
    {
        this.numCols = numCols;
        this.size = size;
        this.maxSize = size;
        this.cols = new ColumnVector[numCols];

        memoryUsage += (long) Integer.BYTES * (size + numCols) +
        Integer.BYTES * 6 + Long.BYTES + 2;

        // Initially all columns are projected and in the same order
        projectionSize = numCols;
    }

    /**
     * Returns the maximum size of the batch (number of rows it can hold)
     */
    public int getMaxSize()
    {
        return maxSize;
    }

    /**
     * Return count of qualifying rows.
     *
     * @return number of rows that have not been filtered out
     */
    public long count()
    {
        return size;
    }

    private static String toUTF8(Object o)
    {
        if (o == null)
        {
            return "\\N"; /* as found in LazySimpleSerDe's nullSequence */
        }
        return o.toString();
    }

    @Override
    public String toString()
    {
        if (size == 0)
        {
            return "";
        }
        StringBuilder b = new StringBuilder();

        for (int i = 0; i < size; i++)
        {
            b.append('[');
            for (int k = 0; k < projectionSize; k++)
            {
                ColumnVector cv = cols[k];
                if (k > 0)
                {
                    b.append(", ");
                }
                if (cv != null)
                {
                    cv.stringifyValue(b, i);
                }
            }
            b.append(']');
            if (i < size - 1)
            {
                b.append('\n');
            }
        }
        return b.toString();
    }

    /**
     * Resets the row batch to default state for the next read.
     * - sets selectedInUse to false
     * - sets size to 0
     * - sets endOfFile to false
     * - resets each column
     * - inits each column
     */
    public void reset()
    {
        size = 0;
        endOfFile = false;
        for (ColumnVector vc : cols)
        {
            if (vc != null)
            {
                vc.reset();
                vc.init();
            }
        }
    }

    /**
     * Set the maximum number of rows in the batch.
     * Data is not preserved.
     */
    public void ensureSize(int rows)
    {
        for (int i = 0; i < cols.length; ++i)
        {
            if (!cols[i].duplicated)
            {
                cols[i].ensureSize(rows, false);
            }
        }
    }

    /**
     * Get the approximate (may be slightly lower than actual)
     * cumulative memory usage, which is more meaningful for GC
     * performance tuning.
     *
     * <br/>
     * <b>NOTE:</b> The memory usage of the column vectors are included.
     * @return
     */
    public long getMemoryUsage()
    {
        long colsUsage = 0;
        for (ColumnVector col : this.cols)
        {
            if (col != null)
            {
                colsUsage += col.getMemoryUsage();
            }
        }
        return memoryUsage + colsUsage;
    }

    @Override
    public void close()
    {
        size = 0;
        endOfFile = false;
        if (this.cols != null)
        {
            for (ColumnVector vc : cols)
            {
                if (vc != null)
                {
                    vc.close();
                }
            }
            this.cols = null;
        }
    }
}
