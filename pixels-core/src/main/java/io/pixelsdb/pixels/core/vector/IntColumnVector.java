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
package io.pixelsdb.pixels.core.vector;

import com.google.flatbuffers.FlatBufferBuilder;
import io.pixelsdb.pixels.core.flat.ColumnVectorFlat;
import io.pixelsdb.pixels.core.flat.IntColumnVectorFlat;
import io.pixelsdb.pixels.core.utils.Bitmap;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a nullable int column vector.
 * This class uses a 32-bit long value to hold the values.
 * In high Java versions such as Java 23, 32-bit integer has comparable operation performance as 64-bit integer.
 * Therefore, using 32-bit column vector for int32 columns saves memory without performance degradation.
 * <p>
 * The vector[] field is public by design for high-performance access in the inner
 * loop of query execution.
 *
 * @author hank
 * @create 2024-12-02
 */
public class IntColumnVector extends ColumnVector
{
    public int[] vector;

    /**
     * Use this constructor by default. All column vectors
     * should normally be the default size.
     */
    public IntColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    /**
     * Don't use this except for testing purposes.
     *
     * @param len the number of rows
     */
    public IntColumnVector(int len)
    {
        super(len);
        vector = new int[len];
        memoryUsage += Integer.BYTES * len;
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
                add(Integer.parseInt(value));
                break;
        }
    }

    @Override
    public void add(boolean value)
    {
        add(value ? 1 : 0);
    }

    @Override
    public void add(int v)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        int index = writeIndex++;
        vector[index] = v;
        isNull[index] = false;
    }

    @Override
    public void add(byte[] value)
    {
        if(checkBytesNull(value))
        {
            return;
        }
        if (value.length != Integer.BYTES)
        {
            throw new IllegalArgumentException("Only byte[4] supported for serialization to int");
        }
        int v = ByteBuffer.wrap(value).getInt();
        add(v);
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
        IntColumnVector otherVector = (IntColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return this.vector[index] == otherVector.vector[otherIndex];
        }
        return false;
    }

    @Override
    public int compareElement(int index, int otherIndex, ColumnVector other)
    {
        IntColumnVector otherVector = (IntColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return this.vector[index] < otherVector.vector[otherIndex] ? -1 :
                    (this.vector[index] == otherVector.vector[otherIndex] ? 0 : 1);
        }
        return this.isNull[index] ? -1 : 1;
    }

    // Fill the column vector with the provided value
    public void fill(int value)
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
    public void addElement(int inputIndex, ColumnVector inputVector)
    {
        int index = writeIndex++;
        if (inputVector.noNulls || !inputVector.isNull[inputIndex])
        {
            isNull[index] = false;
            vector[index] = ((IntColumnVector) inputVector).vector[inputIndex];
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
        // isRepeating should be false and src should be an instance of LongColumnVector.
        // However, we do not check these for performance considerations.
        IntColumnVector source = (IntColumnVector) src;

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
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof IntColumnVector)
        {
            IntColumnVector srcVector = (IntColumnVector) inputVector;
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
    public void ensureSize(int size, boolean preserveData)
    {
        super.ensureSize(size, preserveData);
        if (size > vector.length)
        {
            int[] oldArray = vector;
            vector = new int[size];
            memoryUsage += Integer.BYTES * size;
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

    @Override
    public byte getFlatBufferType()
    {
        return ColumnVectorFlat.IntColumnVectorFlat;
    }

    @Override
    public int serialize(FlatBufferBuilder builder)
    {
        int baseOffset = super.serialize(builder);
        int vectorVectorOffset = IntColumnVectorFlat.createVectorVector(builder, vector);
        IntColumnVectorFlat.startIntColumnVectorFlat(builder);
        IntColumnVectorFlat.addBase(builder, baseOffset);
        IntColumnVectorFlat.addVector(builder, vectorVectorOffset);
        return IntColumnVectorFlat.endIntColumnVectorFlat(builder);
    }

    public static IntColumnVector deserialize(IntColumnVectorFlat flat)
    {
        IntColumnVector vector = new IntColumnVector(flat.base().length());
        for (int i = 0; i < flat.vectorLength(); ++i)
        {
            vector.vector[i] = flat.vector(i);
        }
        vector.deserializeBase(flat.base());
        return vector;
    }
}
