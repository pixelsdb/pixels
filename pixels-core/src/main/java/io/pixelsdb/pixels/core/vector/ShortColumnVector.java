/*
 * Copyright 2026 PixelsDB.
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
import io.pixelsdb.pixels.core.flat.ShortColumnVectorFlat;
import io.pixelsdb.pixels.core.utils.Bitmap;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a nullable short (int16) column vector.
 * This class uses 16-bit integer values.
 *
 * @author gengdy
 * @create 2026-08-07
 */
public class ShortColumnVector extends ColumnVector
{
    public short[] vector;

    public ShortColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    public ShortColumnVector(int len)
    {
        super(len);
        this.vector = new short[len];
        this.memoryUsage += (long) Short.BYTES * len;
    }

    @Override
    public void add(int value)
    {
        add((long) value);
    }

    @Override
    public void add(long value)
    {
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE)
        {
            throw new IllegalArgumentException("SHORT value out of range: " + value);
        }
        add((short) value);
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
                add(Short.parseShort(value));
                break;
        }
    }

    @Override
    public void add(boolean value)
    {
        add(value ? 1 : 0);
    }

    public void add(short value)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        int index = writeIndex++;
        this.vector[index] = value;
        this.isNull[index] = false;
    }
    
    @Override
    public void add(byte[] value)
    {
        if (checkBytesNull(value))
        {
            return;
        }
        if (value.length != Integer.BYTES)
        {
            throw new IllegalArgumentException("Only byte[4] supported for serialization to short");
        }
        short v = ByteBuffer.wrap(value).getShort();
        add(v);
    }

    @Override
    public int[] accumulateHashCode(int[] hashCode)
    {
        requireNonNull(hashCode, "hashCode is null");
        checkArgument(hashCode.length > 0 && hashCode.length <= this.length,
                "the length of hashCode is not in the range [1, length]");
        for (int i = 0; i < hashCode.length; ++i)
        {
            if (!this.isNull[i])
            {
                int value = this.vector[i];
                hashCode[i] = 31 * hashCode[i] + (value ^ (value >>> 16));
            }
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int index, int otherIndex, ColumnVector other)
    {
        ShortColumnVector otherVector = (ShortColumnVector) other;
        return !this.isNull[index] && !otherVector.isNull[otherIndex] &&
                this.vector[index] == otherVector.vector[otherIndex];
    }

    @Override
    public int compareElement(int index, int otherIndex, ColumnVector other)
    {
        ShortColumnVector otherVector = (ShortColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return Short.compare(this.vector[index], otherVector.vector[otherIndex]);
        }
        return this.isNull[index] ? -1 : 1;
    }

    public void fill(short value)
    {
        this.noNulls = true;
        this.isRepeating = true;
        this.vector[0] = value;
    }

    @Override
    public void flatten(boolean selectedInUse, int[] sel, int size)
    {
        flattenPush();
        if (isRepeating)
        {
            isRepeating = false;
            short repeatValue = vector[0];
            if (selectedInUse)
            {
                for (int j = 0; j < size; ++j)
                {
                    vector[sel[j]] = repeatValue;
                }
            }
            else
            {
                Arrays.fill(vector, 0, size, repeatValue);
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
            this.isNull[index] = false;
            this.vector[index] = ((ShortColumnVector) inputVector).vector[inputIndex];
        }
        else
        {
            this.isNull[index] = true;
            this.noNulls = false;
        }
    }

    @Override
    public void addSelected(int[] selected, int offset, int length, ColumnVector src)
    {
        ShortColumnVector source = (ShortColumnVector) src;
        for (int i = offset; i < offset + length; ++i)
        {
            int sourceIndex = selected[i];
            int targetIndex = writeIndex++;
            if (source.isNull[sourceIndex])
            {
                this.isNull[targetIndex] = true;
                this.noNulls = false;
            }
            else
            {
                this.vector[targetIndex] = source.vector[sourceIndex];
                this.isNull[targetIndex] = false;
            }
        }
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof ShortColumnVector)
        {
            ShortColumnVector source = (ShortColumnVector) inputVector;
            this.vector = source.vector;
            this.isNull = source.isNull;
            this.writeIndex = source.writeIndex;
            this.noNulls = source.noNulls;
            this.isRepeating = source.isRepeating;
        }
    }

    @Override
    protected void applyFilter(Bitmap filter, int before)
    {
        checkArgument(!isRepeating,
                "column vector is repeating, flatten before applying filter");
        checkArgument(before > 0 && before <= length,
                "before index is not in the range [1, length]");
        boolean filteredNoNulls = true;
        int targetIndex = 0;
        for (int sourceIndex = filter.nextSetBit(0);
             sourceIndex >= 0 && sourceIndex < before;
             sourceIndex = filter.nextSetBit(sourceIndex + 1), ++targetIndex)
        {
            if (sourceIndex > targetIndex)
            {
                this.vector[targetIndex] = this.vector[sourceIndex];
                this.isNull[targetIndex] = this.isNull[sourceIndex];
            }
            if (this.isNull[targetIndex])
            {
                filteredNoNulls = false;
            }
        }
        this.noNulls = filteredNoNulls;
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
            short[] oldArray = vector;
            vector = new short[size];
            memoryUsage += (long) Short.BYTES * size;
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
        return ColumnVectorFlat.ShortColumnVectorFlat;
    }

    @Override
    public int serialize(FlatBufferBuilder builder)
    {
        int baseOffset = super.serialize(builder);
        int vectorOffset = ShortColumnVectorFlat.createVectorVector(builder, vector);
        ShortColumnVectorFlat.startShortColumnVectorFlat(builder);
        ShortColumnVectorFlat.addBase(builder, baseOffset);
        ShortColumnVectorFlat.addVector(builder, vectorOffset);
        return ShortColumnVectorFlat.endShortColumnVectorFlat(builder);
    }

    public static ShortColumnVector deserialize(ShortColumnVectorFlat flat)
    {
        ShortColumnVector result = new ShortColumnVector(flat.base().length());
        for (int i = 0; i < flat.vectorLength(); ++i)
        {
            result.vector[i] = flat.vector(i);
        }
        result.deserializeBase(flat.base());
        return result;
    }
}
