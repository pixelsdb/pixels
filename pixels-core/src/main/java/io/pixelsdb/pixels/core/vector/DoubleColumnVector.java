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

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * DoubleColumnVector derived from org.apache.hadoop.hive.ql.exec.vector
 * <p>
 * This class represents a nullable double precision floating point column vector.
 * This class will be used for operations on all floating point types (float, double)
 * and as such will use a 64-bit double value to hold the biggest possible value.
 * During copy-in/copy-out, smaller types (i.e. float) will be converted as needed. This will
 * reduce the amount of code that needs to be generated and also will run fast since the
 * machine operates with 64-bit words.
 *
 * Double values in this ColumnVector are stored int a long array by default.
 * Call <code>toDoubleValues()</code> to fill <code>dValues</code> array before reading double values.
 *
 * <p>
 * The vector[] field is public by design for high-performance access in the inner
 * loop of query execution.
 */
public class DoubleColumnVector extends ColumnVector
{
    public long[] vector;
    public static final double NULL_VALUE = Double.NaN;

    /**
     * Use this constructor by default. All column vectors
     * should normally be the default size.
     */
    public DoubleColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    public DoubleColumnVector(int len)
    {
        super(len);
        vector = new long[len];
        Arrays.fill(vector, Double.doubleToLongBits(NULL_VALUE));
        memoryUsage += Long.BYTES * len;
    }

    /**
     * Fill the column vector with the provided value
     * @param value
     */
    public void fill(double value)
    {
        noNulls = true;
        isRepeating = true;
        vector[0] = Double.doubleToLongBits(value);
    }

    /**
     * Simplify vector by brute-force flattening noNulls and isRepeating
     * This can be used to reduce combinatorial explosion of code paths in VectorExpressions
     * with many arguments.
     * @param selectedInUse
     * @param sel
     * @param size
     */
    public void flatten(boolean selectedInUse, int[] sel, int size)
    {
        flattenPush();
        if (isRepeating)
        {
            isRepeating = false;
            long repeatVal = vector[0];
            if (selectedInUse)
            {
                for (int j = 0; j < size; j++)
                {
                    int i = sel[j];
                    vector[i] = repeatVal;
                }
            }
            else
            {
                Arrays.fill(vector, 0, size, repeatVal);
            }
            writeIndex = size;
            flattenRepeatingNulls(selectedInUse, sel, size);
        }
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void add(String value)
    {
        Double v = Double.parseDouble(value);
        add(v);
    }

    @Override
    public void add(float value)
    {
        add((double) value);
    }

    @Override
    public void add(double value)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        int index = writeIndex++;
        vector[index] = Double.doubleToLongBits(value);
        isNull[index] = false;
    }

    @Override
    public void addElement(int inputIndex, ColumnVector inputVector)
    {
        int index = writeIndex++;
        if (inputVector.noNulls || !inputVector.isNull[inputIndex])
        {
            isNull[index] = false;
            vector[index] = ((DoubleColumnVector) inputVector).vector[inputIndex];
        }
        else
        {
            isNull[index] = true;
            noNulls = false;
        }
    }

    @Override
    public void addSelected(int[] selected, int offset, int length, ColumnVector src)
    {
        // isRepeating should be false and src should be an instance of DoubleColumnVector.
        // However, we do not check these for performance considerations.
        DoubleColumnVector source = (DoubleColumnVector) src;

        for (int i = offset; i < offset + length; i++)
        {
            int srcIndex = selected[i], thisIndex = writeIndex++;
            if (source.isNull[srcIndex])
            {
                this.isNull[thisIndex] = true;
                this.noNulls = false;
            }
            else
            {
                this.vector[thisIndex] = source.vector[srcIndex];
                this.isNull[thisIndex] = false;
            }
        }
    }

    @Override
    public int[] accumulateHashCode(int[] hashCode)
    {
        requireNonNull(hashCode, "hashCode is null");
        checkArgument(hashCode.length > 0 && hashCode.length <= this.length, "",
                "the length of hashCode is not in the range [1, length]");
        for (int i = 0; i < hashCode.length; ++i)
        {
            if (this.isNull[i])
            {
                continue;
            }
            hashCode[i] = 31 * hashCode[i] + (int)(this.vector[i] ^ (this.vector[i] >>> 32));
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int index, int otherIndex, ColumnVector other)
    {
        DoubleColumnVector otherVector = (DoubleColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return this.vector[index] == otherVector.vector[otherIndex];
        }
        return false;
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof DoubleColumnVector)
        {
            DoubleColumnVector srcVector = (DoubleColumnVector) inputVector;
            this.vector = srcVector.vector;
            this.isNull = srcVector.isNull;
            this.writeIndex = srcVector.writeIndex;
            this.noNulls = srcVector.noNulls;
            this.isRepeating = srcVector.isRepeating;
        }
    }

    @Override
    protected void applyFilter(Bitmap filter, int before)
    {
        checkArgument(!isRepeating,
                "column vector is repeating, flatten before applying filter");
        checkArgument(before > 0 && before <= length,
                "before index is not in the range [1, length]");
        boolean noNulls = true;
        int j = 0;
        for (int i = filter.nextSetBit(0);
             i >= 0 && i < before; i = filter.nextSetBit(i+1), j++)
        {
            if (i > j)
            {
                this.vector[j] = this.vector[i];
                this.isNull[j] = this.isNull[i];
            }
            if (this.isNull[j])
            {
                noNulls = false;
            }
            /*
             * The number of rows in a row batch is impossible to reach Integer.MAX_VALUE.
             * Therefore, we do not check overflow here.
             */
        }
        this.noNulls = noNulls;
    }

    @Override
    public void stringifyValue(StringBuilder buffer, int row)
    {
        if (isRepeating)
        {
            row = 0;
        }
        if (noNulls || !isNull[row])
        {
            buffer.append(vector[row]);
        }
        else
        {
            buffer.append("null");
        }
    }

    @Override
    public void close()
    {
        super.close();
        this.vector = null;
    }

    @Override
    public void reset()
    {
        super.reset();
        Arrays.fill(vector, Double.doubleToLongBits(NULL_VALUE));
    }

    @Override
    public void ensureSize(int size, boolean preserveData)
    {
        super.ensureSize(size, preserveData);
        if (size > vector.length)
        {
            long[] oldArray = vector;
            vector = new long[size];
            Arrays.fill(vector, Double.doubleToLongBits(NULL_VALUE));
            memoryUsage += Long.BYTES * size;
            length = size;
            if (preserveData)
            {
                if (isRepeating)
                {
                    vector[0] = oldArray[0];
                }
                else
                {
                    System.arraycopy(oldArray, 0, vector, 0, oldArray.length);
                }
            }
        }
    }
}
