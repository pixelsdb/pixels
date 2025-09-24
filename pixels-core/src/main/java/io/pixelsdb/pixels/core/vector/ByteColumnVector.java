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
import io.pixelsdb.pixels.core.flat.ByteColumnVectorFlat;
import io.pixelsdb.pixels.core.flat.ColumnVectorFlat;
import io.pixelsdb.pixels.core.utils.Bitmap;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

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
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        int index = writeIndex++;
        vector[index] = value;
        isNull[index] = false;
    }

    @Override
    public void add(byte[] value)
    {
        if(checkBytesNull(value))
        {
            return;
        }
        if (value.length != Byte.BYTES)
        {
            throw new IllegalArgumentException("Only byte arrays of length 1 are supported, got length: " + value.length);
        }
        add(value[0]);
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
            hashCode[i] = 31 * hashCode[i] + this.vector[i];
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int index, int otherIndex, ColumnVector other)
    {
        ByteColumnVector otherVector = (ByteColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return this.vector[index] == otherVector.vector[otherIndex];
        }
        return false;
    }

    @Override
    public int compareElement(int index, int otherIndex, ColumnVector other)
    {
        ByteColumnVector otherVector = (ByteColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return Byte.compare(this.vector[index], otherVector.vector[otherIndex]);
        }
        return this.isNull[index] ? -1 : 1;
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
            vector[index] = ((ByteColumnVector) inputVector).vector[inputIndex];
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
        // isRepeating should be false and src should be an instance of ByteColumnVector.
        // However, we do not check these for performance considerations.
        ByteColumnVector source = (ByteColumnVector) src;

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

    @Override
    public byte getFlatBufferType()
    {
        return ColumnVectorFlat.ByteColumnVectorFlat;
    }

    @Override
    public int serialize(FlatBufferBuilder builder)
    {
        int baseOffsets = super.serialize(builder);

        int vectorVector = ByteColumnVectorFlat.createVectorVector(builder, vector);
        ByteColumnVectorFlat.startByteColumnVectorFlat(builder);
        ByteColumnVectorFlat.addBase(builder, baseOffsets);
        ByteColumnVectorFlat.addVector(builder, vectorVector);
        return ByteColumnVectorFlat.endByteColumnVectorFlat(builder);
    }

    public static ByteColumnVector deserialize(ByteColumnVectorFlat flat)
    {
        ByteColumnVector vector = new ByteColumnVector(flat.base().length());
        for (int i = 0; i < flat.vectorLength(); ++i)
        {
            vector.vector[i] = flat.vector(i);
        }
        vector.deserializeBase(flat.base());
        return vector;
    }
}
