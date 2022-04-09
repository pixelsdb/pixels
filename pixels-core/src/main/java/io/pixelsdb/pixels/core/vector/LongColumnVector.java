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

/**
 * LongColumnVector derived from org.apache.hadoop.hive.ql.exec.vector
 * <p>
 * This class represents a nullable int column vector.
 * This class will be used for operations on all integer types (tinyint, smallint, int, bigint)
 * and as such will use a 64-bit long value to hold the biggest possible value.
 * During copy-in/copy-out, smaller int types will be converted as needed. This will
 * reduce the amount of code that needs to be generated and also will run fast since the
 * machine operates with 64-bit words.
 * <p>
 * The vector[] field is public by design for high-performance access in the inner
 * loop of query execution.
 */
public class LongColumnVector extends ColumnVector
{
    public long[] vector;

    /**
     * Use this constructor by default. All column vectors
     * should normally be the default size.
     */
    public LongColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    /**
     * Don't use this except for testing purposes.
     *
     * @param len the number of rows
     */
    public LongColumnVector(int len)
    {
        super(len);
        vector = new long[len];
        memoryUsage += Long.BYTES * len;
    }

    @Override
    public void add(String value)
    {
        switch (value.toLowerCase())
        {
            case "true":
                add(1);
                break;
            case "false":
                add(0);
                break;
            default:
                add(Long.parseLong(value));
                break;
        }
    }

    @Override
    public void add(boolean value)
    {
        add(value ? 1 : 0);
    }

    @Override
    public void add(long v)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        vector[writeIndex++] = v;
    }

    // Copy the current object contents into the output. Only copy selected entries,
    // as indicated by selectedInUse and the sel array.
    public void copySelected(
            boolean selectedInUse, int[] sel, int size, LongColumnVector output)
    {
        // Output has nulls if and only if input has nulls.
        output.noNulls = noNulls;
        output.isRepeating = false;

        // Handle repeating case
        if (isRepeating)
        {
            output.vector[0] = vector[0];
            output.isNull[0] = isNull[0];
            output.isRepeating = true;
            return;
        }

        // Handle normal case

        // Copy data values over
        if (selectedInUse)
        {
            for (int j = 0; j < size; j++)
            {
                int i = sel[j];
                output.vector[i] = vector[i];
            }
        }
        else
        {
            System.arraycopy(vector, 0, output.vector, 0, size);
        }

        // Copy nulls over if needed
        if (!noNulls)
        {
            if (selectedInUse)
            {
                for (int j = 0; j < size; j++)
                {
                    int i = sel[j];
                    output.isNull[i] = isNull[i];
                }
            }
            else
            {
                System.arraycopy(isNull, 0, output.isNull, 0, size);
            }
        }
    }

    // Fill the column vector with the provided value
    public void fill(long value)
    {
        noNulls = true;
        isRepeating = true;
        vector[0] = value;
    }

    // Simplify vector by brute-force flattening noNulls and isRepeating
    // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
    // with many arguments.
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
            flattenRepeatingNulls(selectedInUse, sel, size);
        }
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector)
    {
        if (inputVector.isRepeating)
        {
            inputElementNum = 0;
        }
        if (inputVector.noNulls || !inputVector.isNull[inputElementNum])
        {
            isNull[outElementNum] = false;
            vector[outElementNum] =
                    ((LongColumnVector) inputVector).vector[inputElementNum];
        }
        else
        {
            isNull[outElementNum] = true;
            noNulls = false;
        }
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof LongColumnVector)
        {
            LongColumnVector srcVector = (LongColumnVector) inputVector;
            this.vector = srcVector.vector;
            this.isNull = srcVector.isNull;
            this.writeIndex = srcVector.writeIndex;
            this.noNulls = srcVector.noNulls;
            this.isRepeating = srcVector.isRepeating;
        }
    }

    @Override
    protected void applyFilter(Bitmap filter, int beforeIndex)
    {
        checkArgument(!isRepeating,
                "column vector is repeating, flatten before applying filter");

        boolean noNulls = true;
        for (int i = filter.nextSetBit(0), j = 0;
             i >= 0 && i < this.length; i = filter.nextSetBit(i+1), j++)
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
    public void ensureSize(int size, boolean preserveData)
    {
        super.ensureSize(size, preserveData);
        if (size > vector.length)
        {
            long[] oldArray = vector;
            vector = new long[size];
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

    @Override
    public void close()
    {
        super.close();
        this.vector = null;
    }
}
