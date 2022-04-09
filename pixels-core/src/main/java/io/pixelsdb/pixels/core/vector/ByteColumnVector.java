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
 * ByteColumnVector
 * <p>
 *     This class represents a nullable byte column vector.
 *     It can be used for operations on all boolean/byte types
 * </p>
 *
 * @author guodong
 */
public class ByteColumnVector extends ColumnVector
{
    public byte[] vector;

    public ByteColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    public ByteColumnVector(int len)
    {
        super(len);
        vector = new byte[len];
        memoryUsage += Byte.BYTES * len;
    }

    @Override
    public void add(boolean value)
    {
        add(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public void add(byte value)
    {
        vector[writeIndex++] = value;
    }

    @Override
    public void add(String value)
    {
        assert value != null && value.length() > 0;
        char c = value.charAt(0);
        if (c == '0' || c == '1')
        {
            add(Byte.parseByte(value));
        }
        else
        {
            add(Boolean.parseBoolean(value));
        }
    }

    @Override
    public void flatten(boolean selectedInUse, int[] sel, int size)
    {
        flattenPush();
        if (isRepeating)
        {
            isRepeating = false;
            byte repeatVal = vector[0];
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
                    ((ByteColumnVector) inputVector).vector[inputElementNum];
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
        if (inputVector instanceof ByteColumnVector)
        {
            ByteColumnVector srcVector = (ByteColumnVector) inputVector;
            this.vector = srcVector.vector;
            this.isNull = srcVector.isNull;
            this.writeIndex = srcVector.writeIndex;
            this.noNulls = srcVector.noNulls;
            this.isRepeating = srcVector.isRepeating;
        }
    }

    @Override
    protected void applyFilter(Bitmap filter)
    {
        checkArgument(!isRepeating, "column vector is repeating, flatten before applying filter");

        int j = 0;
        boolean noNulls = true;
        for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i+1))
        {
            if (i > j)
            {
                this.vector[j] = this.vector[i];
                this.isNull[j] = this.isNull[i];
                if (this.isNull[j])
                {
                    noNulls = false;
                }
                j++;
            }
            /*
             * The number of rows in a row batch is impossible to reach Integer.MAX_VALUE.
             * Therefore, we do not check overflow here.
             */
        }
        this.noNulls = noNulls;
        this.length = j;
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
    public void ensureSize(int size, boolean preserveData)
    {
        super.ensureSize(size, preserveData);
        if (size > vector.length)
        {
            byte[] oldArray = vector;
            vector = new byte[size];
            memoryUsage += Byte.BYTES * size;
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
