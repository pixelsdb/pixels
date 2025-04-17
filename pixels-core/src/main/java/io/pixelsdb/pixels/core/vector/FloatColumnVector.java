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
package io.pixelsdb.pixels.core.vector;

import com.google.flatbuffers.FlatBufferBuilder;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.utils.flat.FloatColumnVectorFlat;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * <p>
 * This class represents a nullable single precision floating point column vector.
 * This class uses a 32-bit double value to hold the biggest possible value.
 * Float values in this ColumnVector are stored as an int array by default.
 * <p>
 * The vector[] field is public by design for high-performance access in the inner
 * loop of query execution.
 *
 * @author hank
 * @create 2023-08-20 Avry
 */
public class FloatColumnVector extends ColumnVector
{
    public int[] vector;
    public static final float NULL_VALUE = Float.NaN;

    /**
     * Use this constructor by default. All column vectors
     * should normally be the default size.
     */
    public FloatColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    public FloatColumnVector(int len)
    {
        super(len);
        vector = new int[len];
        Arrays.fill(vector, Float.floatToIntBits(NULL_VALUE));
        memoryUsage += (long) Integer.BYTES * len;
    }

    /**
     * Fill the column vector with the provided value.
     * @param value the value used to fill
     */
    public void fill(float value)
    {
        noNulls = true;
        isRepeating = true;
        vector[0] = Float.floatToIntBits(value);
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
            int repeatVal = vector[0];
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
        add(Float.parseFloat(value));
    }

    @Override
    public void add(float value)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        int index = writeIndex++;
        vector[index] = Float.floatToIntBits(value);
        isNull[index] = false;
    }

    @Override
    public void add(double value)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        if (value < Float.MIN_VALUE || value > Float.MAX_VALUE)
        {
            throw new IllegalArgumentException("value " + value + " is out of the range of a float");
        }
        int index = writeIndex++;
        vector[index] = Float.floatToIntBits((float) value);
        isNull[index] = false;
    }

    @Override
    public void addElement(int inputIndex, ColumnVector inputVector)
    {
        int index = writeIndex++;
        if (inputVector.noNulls || !inputVector.isNull[inputIndex])
        {
            isNull[index] = false;
            vector[index] = ((FloatColumnVector) inputVector).vector[inputIndex];
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
        FloatColumnVector source = (FloatColumnVector) src;

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
            hashCode[i] = 31 * hashCode[i] + (this.vector[i] ^ (this.vector[i] >>> 16));
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int index, int otherIndex, ColumnVector other)
    {
        FloatColumnVector otherVector = (FloatColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return this.vector[index] == otherVector.vector[otherIndex];
        }
        return false;
    }

    @Override
    public int compareElement(int index, int otherIndex, ColumnVector other)
    {
        FloatColumnVector otherVector = (FloatColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return Integer.compare(this.vector[index], otherVector.vector[otherIndex]);
        }
        return this.isNull[index] ? -1 : 1;
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof FloatColumnVector)
        {
            FloatColumnVector srcVector = (FloatColumnVector) inputVector;
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
        // Issue #367: We should not rely on the vector to distinguish null values.
        // Arrays.fill(vector, Float.floatToIntBits(NULL_VALUE));
    }

    @Override
    public void ensureSize(int size, boolean preserveData)
    {
        super.ensureSize(size, preserveData);
        if (size > vector.length)
        {
            int[] oldArray = vector;
            vector = new int[size];
            Arrays.fill(vector, Float.floatToIntBits(NULL_VALUE));
            memoryUsage += (long) Integer.BYTES * size;
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
    public int serialize(FlatBufferBuilder builder)
    {
        int baseOffset = super.serialize(builder);

        FloatColumnVectorFlat.startFloatColumnVectorFlat(builder);
        FloatColumnVectorFlat.addBase(builder, baseOffset);
        FloatColumnVectorFlat.addVector(builder, FloatColumnVectorFlat.createVectorVector(builder, vector));
        return FloatColumnVectorFlat.endFloatColumnVectorFlat(builder);
    }

    public static FloatColumnVector deserialize(FloatColumnVectorFlat flat)
    {
        FloatColumnVector vector = new FloatColumnVector(flat.base().length());
        for (int i = 0; i < flat.vectorLength(); ++i)
        {
            vector.vector[i] = flat.vector(i);
        }
        vector.deserializeBase(flat.base());
        return vector;
    }
}
