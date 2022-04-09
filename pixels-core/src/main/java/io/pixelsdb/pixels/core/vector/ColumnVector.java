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

import io.pixelsdb.pixels.core.utils.Bitmap;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * ColumnVector derived from org.apache.hadoop.hive.ql.exec.vector.
 * <p>
 * ColumnVector contains the shared structure for the sub-types,
 * including NULL information, and whether this vector
 * repeats, i.e. has all values the same, so only the first
 * one is set. This is used to accelerate query performance
 * by handling a whole vector in O(1) time when applicable.
 * <p>
 * The fields are public by design since this is a performance-critical
 * structure that is used in the inner loop of query execution.
 */
public abstract class ColumnVector implements AutoCloseable
{
    /**
     * length is the capacity, i.e., maximum number of values, of this column vector
     * <b>DO NOT</b> modify it or used it as the number of values in-used.
     */
    int length;
    int writeIndex = 0;
    long memoryUsage = 0L;

    /**
     * The current kinds of column vectors.
     */
    public static enum Type
    {
        NONE,    // Useful when the type of column vector has not be determined yet.
        LONG,
        DOUBLE,
        BYTES,
        DECIMAL,
        TIMESTAMP,
        INTERVAL_DAY_TIME,
        STRUCT,
        LIST,
        MAP,
        UNION
    }

    /**
     * If this column vector is a duplication of another column vector
     */
    public boolean duplicated = false;

    /**
     * The id of the origin column vector
     */
    public int originVecId = -1;

    /**
     * If hasNulls is true, then this array contains true if the value
     * is null, otherwise false. The array is always allocated, so a batch can be re-used
     * later and nulls added.
     */
    public boolean[] isNull;

    // If the whole column vector has no nulls, this is true, otherwise false.
    public boolean noNulls;

    /**
     * True if same value repeats for whole column vector.
     * If so, vector[0] holds the repeating value.
     */
    public boolean isRepeating;

    // Variables to hold state from before flattening so it can be easily restored.
    private boolean preFlattenIsRepeating;
    private boolean preFlattenNoNulls;

    /**
     * Constructor for super-class ColumnVector. This is not called directly,
     * but used to initialize inherited fields.
     *
     * @param len Vector length
     */
    public ColumnVector(int len)
    {
        this.length = len;
        isNull = new boolean[len];
        memoryUsage += len + Integer.BYTES*3 + 4;
        noNulls = true;
        isRepeating = false;
        preFlattenNoNulls = true;
        preFlattenIsRepeating = false;
    }

    public void add(boolean value)
    {
        throw new UnsupportedOperationException("Adding boolean is not supported");
    }

    public void add(byte value)
    {
        throw new UnsupportedOperationException("Adding byte is not supported");
    }

    public void add(byte[] value)
    {
        throw new UnsupportedOperationException("Adding bytes is not supported");
    }

    public void add(double value)
    {
        throw new UnsupportedOperationException("Adding double is not supported");
    }

    public void add(float value)
    {
        throw new UnsupportedOperationException("Adding float is not supported");
    }

    public void add(long value)
    {
        throw new UnsupportedOperationException("Adding long is not supported");
    }

    public void add(String value)
    {
        throw new UnsupportedOperationException("Adding string is not supported");
    }

    public void add(Date value)
    {
        throw new UnsupportedOperationException("Adding date is not supported");
    }

    public void add(Time value)
    {
        throw new UnsupportedOperationException("Adding time is not supported");
    }

    public void add(Timestamp value)
    {
        throw new UnsupportedOperationException("Adding timestamp is not supported");
    }

    public int getLength()
    {
        return length;
    }

    /**
     * Get the approximate (may be slightly lower than actual)
     * cumulative memory usage, which is more meaningful for GC
     * performance tuning.
     *
     * <br/>
     * <b>NOTE:</b> Only the heap memory allocated internally are
     * counted. The memory usage of the external parameters are not included.
     * @return
     */
    public long getMemoryUsage()
    {
        return memoryUsage;
    }

    /**
     * Resets the column to default state
     * - fills the isNull array with false
     * - sets noNulls to true
     * - sets isRepeating to false
     */
    public void reset()
    {
        if (!noNulls)
        {
            Arrays.fill(isNull, false);
        }
        noNulls = true;
        isRepeating = false;
        preFlattenNoNulls = true;
        preFlattenIsRepeating = false;
        writeIndex = 0;
    }

    /**
     * Sets the isRepeating flag. Recurses over structs and unions so that the
     * flags are set correctly.
     *
     * @param isRepeating
     */
    public void setRepeating(boolean isRepeating)
    {
        this.isRepeating = isRepeating;
    }

    abstract public void flatten(boolean selectedInUse, int[] sel, int size);

    /**
     * Simplify vector by brute-force flattening noNulls if isRepeating
     * This can be used to reduce combinatorial explosion of code paths in VectorExpressions
     * with many arguments.
     *
     * @param selectedInUse if set true, it means part of the vector is selected in use
     * @param sel           in use selection array
     * @param size          in use size
     */
    protected void flattenRepeatingNulls(boolean selectedInUse, int[] sel,
                                         int size)
    {
        boolean nullFillValue;

        if (noNulls)
        {
            nullFillValue = false;
        }
        else
        {
            nullFillValue = isNull[0];
        }

        if (selectedInUse)
        {
            for (int j = 0; j < size; j++)
            {
                int i = sel[j];
                isNull[i] = nullFillValue;
            }
        }
        else
        {
            Arrays.fill(isNull, 0, size, nullFillValue);
        }

        // all nulls are now explicit
        noNulls = false;
    }

    protected void flattenNoNulls(boolean selectedInUse, int[] sel,
                                  int size)
    {
        if (noNulls)
        {
            noNulls = false;
            if (selectedInUse)
            {
                for (int j = 0; j < size; j++)
                {
                    isNull[sel[j]] = false;
                }
            }
            else
            {
                Arrays.fill(isNull, 0, size, false);
            }
        }
    }

    /**
     * Restore the state of isRepeating and noNulls to what it was
     * before flattening. This must only be called just after flattening
     * and then evaluating a VectorExpression on the column vector.
     * It is an optimization that allows other operations on the same
     * column to continue to benefit from the isRepeating and noNulls
     * indicators.
     */
    public void unFlatten()
    {
        isRepeating = preFlattenIsRepeating;
        noNulls = preFlattenNoNulls;
    }

    // Record repeating and no nulls state to be restored later.
    protected void flattenPush()
    {
        preFlattenIsRepeating = isRepeating;
        preFlattenNoNulls = noNulls;
    }

    /**
     * Set the element in this column vector from the given input vector.
     * This method can assume that the output does not have isRepeating set.
     */
    public abstract void setElement(int outElementNum, int inputElementNum,
                                    ColumnVector inputVector);

    /**
     * Shallow copy from input vector.
     * This is used for duplicated reference column vector.
     * This method does not provide deep cloning of vector content.
     */
    public abstract void duplicate(ColumnVector inputVector);

    /**
     * Initialize the column vector. This method can be overridden by specific column
     * vector types. Use this method only if the individual type of the column vector
     * is not known, otherwise it is preferable to call specific initialization methods.
     */
    public void init()
    {
        // Do nothing by default
    }

    /**
     * Compact all the survived values before the give beforeIndex (exclusive) to the
     * front of this column vector. The ith value survives if the ith bit in filter is set.
     * @param filter the filter
     * @param beforeIndex the exclusive index before which the rows are checked
     */
    abstract protected void applyFilter(Bitmap filter, int beforeIndex);

    /**
     * Ensure the ColumnVector can hold at least size values.
     * This method is deliberately *not* recursive because the complex types
     * can easily have more (or less) children than the upper levels.
     *
     * @param size         the new minimum size
     * @param preserveData should the old data be preserved?
     */
    public void ensureSize(int size, boolean preserveData)
    {
        if (isNull.length < size)
        {
            boolean[] oldArray = isNull;
            isNull = new boolean[size];
            memoryUsage += size;
            if (preserveData && !noNulls)
            {
                if (isRepeating)
                {
                    isNull[0] = oldArray[0];
                }
                else
                {
                    System.arraycopy(oldArray, 0, isNull, 0, oldArray.length);
                }
            }
        }
    }

    /**
     * Print the value for this column into the given string builder.
     *
     * @param buffer the buffer to print into
     * @param row    the id of the row to print
     */
    public abstract void stringifyValue(StringBuilder buffer,
                                        int row);

    @Override
    public void close()
    {
        isNull = null;
        noNulls = true;
        isRepeating = false;
        preFlattenNoNulls = true;
        preFlattenIsRepeating = false;
        writeIndex = 0;
    }
}
