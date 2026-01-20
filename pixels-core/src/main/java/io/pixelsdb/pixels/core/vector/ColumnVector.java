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

import com.google.flatbuffers.FlatBufferBuilder;
import io.pixelsdb.pixels.core.flat.ColumnVectorBaseFlat;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.utils.Integer128;

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
     * True if same value repeats for whole column vector.
     * If so, vector[0] holds the repeating value.
     * <p/>
     * <b>Note</b> that if isRepeating is true, <b>NONE</b> of the methods in this class,
     * except flatten/ duplicate/ stringifyValue/ reset/ ensureSize/ getLength/ getMemoryUsage,
     * should be used before calling flatten().
     */
    boolean isRepeating;

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
        memoryUsage += len + Integer.BYTES * 3 + 4;
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

    public void add(int value)
    {
        throw new UnsupportedOperationException("Adding int is not supported");
    }

    public void add(long value)
    {
        throw new UnsupportedOperationException("Adding long is not supported");
    }

    public void add(Integer128 value)
    {
        throw new UnsupportedOperationException("Adding Integer128 is not supported");
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

    public void add(double[] vector) {throw new UnsupportedOperationException("Adding vector is not supported"); }

    public void addNull()
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        this.isNull[writeIndex++] = true;
        this.noNulls = false;
    }

    public void pushWriteIndex(int delta)
    {
        if (writeIndex + delta <= length)
        {
            // writeIndex == length is valid, meaning this column vector is full.
            writeIndex += delta;
        }
        else
        {
            throw new IndexOutOfBoundsException(Integer.toString(writeIndex + delta));
        }
    }

    public void setWriteIndex(int writeIndex)
    {
        if (writeIndex <= length)
        {
            // writeIndex == length is valid, meaning this column vector is full.
            this.writeIndex = writeIndex;
        }
        else
        {
            throw new IndexOutOfBoundsException(Integer.toString(writeIndex));
        }
    }

    /**
     * Add the element from the given input vector into this column vector.
     * This method can assume that the output does not have isRepeating set.
     */
    public abstract void addElement(int inputIndex, ColumnVector inputVector);

    /**
     * Add the selected elements in the source column vector into this column vector.
     *
     * @param selected the index of the selected elements in src
     * @param offset the starting offset in selected
     * @param length the length in selected
     * @param src the source column vector
     */
    public abstract void addSelected(int[] selected, int offset, int length, ColumnVector src);

    public int getLength()
    {
        return length;
    }

    public boolean isRepeating()
    {
        return isRepeating;
    }

    public int getWriteIndex()
    {
        return writeIndex;
    }

    /**
     * Get the approximate (maybe slightly lower than actual)
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
     * Get the accumulative hash code of the elements in this column vector.
     * For ith element in this column vector, the hash code is computed as:
     * <blockquote>
     *     hashCode[i] = 31 * hashCode[i] + vector[i].hashCode()
     * </blockquote>
     * If you need the exact hash code of this vector, fill hashCode by 0;
     *
     * @param hashCode the array to store the hash code
     * @return the same int array in the input
     */
    public abstract int[] accumulateHashCode(int[] hashCode);

    /**
     * Whether the two elements in this and the other column vector equals.
     * <b>Note</b> that two elements are not equal if either or both of them is(are) null.
     *
     * @param index the index in this column vector
     * @param otherIndex the index in the other column vector
     * @param other the other column vector
     * @return true if the two elements equals, otherwise returns false
     */
    public abstract boolean elementEquals(int index, int otherIndex, ColumnVector other);

    /**
     * Compare the elements in this and the other column vector.
     * <b>Note</b> that null value is considered smaller than any values including null.
     *
     * @param index the index in this column vector
     * @param otherIndex the index in the other column vector
     * @param other the other column vector
     * @return 1 if the element in this vector is larger, 0 if the two elements are equivalent, and
     * -1 if the element in this vector is smaller.
     */
    public abstract int compareElement(int index, int otherIndex, ColumnVector other);

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
     * @param before the exclusive index before which the rows are checked
     */
    abstract protected void applyFilter(Bitmap filter, int before);

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
    public abstract void stringifyValue(StringBuilder buffer, int row);

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

    /**
     * Get the flatBuffer type for this column vector
     * @return
     */
    public abstract byte getFlatBufferType();

    /**
     * Serialize this column vector
     * @param builder
     * @return
     */
    public int serialize(FlatBufferBuilder builder)
    {
        int isNullVectorOffset = ColumnVectorBaseFlat.createIsNullVector(builder, isNull);
        ColumnVectorBaseFlat.startColumnVectorBaseFlat(builder);
        ColumnVectorBaseFlat.addLength(builder, length);
        ColumnVectorBaseFlat.addWriteIndex(builder, writeIndex);
        ColumnVectorBaseFlat.addMemoryUsage(builder, memoryUsage);
        ColumnVectorBaseFlat.addIsRepeating(builder, isRepeating);
        ColumnVectorBaseFlat.addDuplicated(builder, duplicated);
        ColumnVectorBaseFlat.addOriginVecId(builder, originVecId);
        ColumnVectorBaseFlat.addIsNull(builder, isNullVectorOffset);
        ColumnVectorBaseFlat.addNoNulls(builder, noNulls);
        ColumnVectorBaseFlat.addPreFlattenIsRepeating(builder, preFlattenIsRepeating);
        ColumnVectorBaseFlat.addPreFlattenNoNulls(builder, preFlattenNoNulls);
        return ColumnVectorBaseFlat.endColumnVectorBaseFlat(builder);
    }

    /**
     * Deserialize the base column vector
     * @param base
     */
    protected void deserializeBase(ColumnVectorBaseFlat base)
    {
        this.length = base.length();
        this.writeIndex = base.writeIndex();
        this.memoryUsage = base.memoryUsage();
        this.isRepeating = base.isRepeating();
        this.duplicated = base.duplicated();
        this.originVecId = base.originVecId();
        for (int i = 0; i < base.isNullLength(); ++i)
        {
            this.isNull[i] = base.isNull(i);
        }
        this.noNulls = base.noNulls();
        this.preFlattenIsRepeating = base.preFlattenIsRepeating();
        this.preFlattenNoNulls = base.preFlattenNoNulls();
    }

    /**
     * Check if the given byte array is null.
     * If null, mark the current write index as null and ensure enough capacity.
     *
     * @param value the byte array to check
     * @return true if the value is null and has been marked; false otherwise
     */
    protected boolean checkBytesNull(byte[] value)
    {
        if (value == null || value.length == 0)
        {
            if(writeIndex >= getLength())
            {
                ensureSize(writeIndex * 2, true);
            }
            isNull[writeIndex++] = true;
            return true;
        }
        return false;
    }
}
