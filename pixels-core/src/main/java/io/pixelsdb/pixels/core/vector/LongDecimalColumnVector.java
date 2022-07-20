/*
 * Copyright 2022 PixelsDB.
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
import io.pixelsdb.pixels.core.utils.Integer128;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.core.TypeDescription.LONG_DECIMAL_MAX_PRECISION;
import static io.pixelsdb.pixels.core.TypeDescription.LONG_DECIMAL_MAX_SCALE;
import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.util.Objects.requireNonNull;

/**
 * This class is similar to {@link DecimalColumnVector}, but supports long decimals
 * with max precision and scale 38.
 *
 * Created at: 01/07/2022
 * Author: hank
 */
public class LongDecimalColumnVector extends ColumnVector
{
    public static final ByteOrder ENDIAN = ByteOrder.BIG_ENDIAN;
    public static final long DEFAULT_UNSCALED_VALUE = 0;
    public long[] vector;
    private int precision;
    private int scale;

    public LongDecimalColumnVector(int precision, int scale)
    {
        this(VectorizedRowBatch.DEFAULT_SIZE, precision, scale);
    }

    public LongDecimalColumnVector(int len, int precision, int scale)
    {
        super(len);
        vector = new long[2*len];
        Arrays.fill(vector, DEFAULT_UNSCALED_VALUE);
        memoryUsage += (long) Long.BYTES * len * 2 + Integer.BYTES * 2;

        if (precision < 1)
        {
            throw new IllegalArgumentException("precision " + precision + " is negative");
        }
        else if (precision > LONG_DECIMAL_MAX_PRECISION)
        {
            throw new IllegalArgumentException("precision " + precision +
                    " is out of the max precision " + LONG_DECIMAL_MAX_PRECISION);
        }
        this.precision = precision;

        if (scale < 0)
        {
            throw new IllegalArgumentException("scale " + scale + " is negative");
        }
        else if (scale > LONG_DECIMAL_MAX_SCALE)
        {
            throw new IllegalArgumentException("scale " + scale +
                    " is out of the max scale " + LONG_DECIMAL_MAX_SCALE);
        }
        else if (scale > precision)
        {
            throw new IllegalArgumentException("precision " + precision +
                    " is smaller that scale " + scale);
        }
        this.scale = scale;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    /**
     * Fill the column vector with the provided value
     * @param value
     */
    public void fill(double value)
    {
        noNulls = true;
        isRepeating = true;
        BigDecimal decimal = BigDecimal.valueOf(value);
        BigInteger unscaled = decimal.unscaledValue();
        vector[1] = unscaled.longValue();
        vector[0] = unscaled.shiftRight(64).longValueExact();
        this.scale = decimal.scale();
        this.precision = decimal.precision();
    }

    /**
     * Simplify vector by brute-force flattening noNulls and isRepeating
     * This can be used to reduce combinatorial explosion of code paths in VectorExpressions
     * with many arguments.
     * @param selectedInUse
     * @param sel
     * @param size
     */
    @Override
    public void flatten(boolean selectedInUse, int[] sel, int size)
    {
        flattenPush();
        if (isRepeating)
        {
            isRepeating = false;
            long repeatHigh = vector[0];
            long repeatLow = vector[1];
            if (selectedInUse)
            {
                for (int j = 0; j < size; j++)
                {
                    int i = sel[j];
                    vector[i*2] = repeatHigh;
                    vector[i*2+1] = repeatLow;
                }
            }
            else
            {
                for (int i = 1; i < size; i++)
                {
                    vector[i*2] = repeatHigh;
                    vector[i*2+1] = repeatLow;
                }
            }
            writeIndex = size;
            flattenRepeatingNulls(selectedInUse, sel, size);
        }
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void add(Integer128 value)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        int index = writeIndex++;
        vector[index*2] = value.getHigh();
        vector[index*2+1] = value.getLow();
        isNull[index] = false;
    }

    @Override
    public void add(String value)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        // Convert to a BigDecimal with unlimited precision and HALF_UP rounding.
        BigDecimal decimal = new BigDecimal(value, MathContext.UNLIMITED);
        if (decimal.scale() != scale)
        {
            decimal = decimal.setScale(scale, ROUND_HALF_UP);
        }
        if (decimal.precision() > precision)
        {
            throw new IllegalArgumentException("value exceeds the allowed precision " + precision);
        }
        int index = writeIndex++;
        // We use big endian.
        BigInteger unscaledValue = decimal.unscaledValue();
        vector[index*2+1] = unscaledValue.longValue();
        vector[index*2] = unscaledValue.shiftRight(64).longValueExact();
        isNull[index] = false;
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
        // Decimal.valueOf converts double to Decimal with unlimited precision and HALF_UP rounding.
        BigDecimal decimal = BigDecimal.valueOf(value);
        if (decimal.scale() != scale)
        {
            decimal = decimal.setScale(scale, ROUND_HALF_UP);
        }
        if (decimal.precision() > precision)
        {
            throw new IllegalArgumentException("value exceeds the allowed precision " + precision);
        }
        int index = writeIndex++;
        // We use big endian.
        BigInteger unscaledValue = decimal.unscaledValue();
        vector[index*2+1] = unscaledValue.longValue();
        vector[index*2] = unscaledValue.shiftRight(64).longValueExact();
        isNull[index] = false;
    }

    public BigDecimal getScratchDecimal(int index)
    {
        BigInteger unscaled = new BigInteger(Integer128.toBigEndianBytes(vector[index*2], vector[index*2+1]));
        return new BigDecimal(unscaled, scale);
    }

    @Override
    public void addElement(int inputIndex, ColumnVector inputVector)
    {
        int index = writeIndex++;
        if (inputVector.noNulls || !inputVector.isNull[inputIndex])
        {
            isNull[index] = false;
            LongDecimalColumnVector src = ((LongDecimalColumnVector) inputVector);
            vector[index*2] = src.vector[inputIndex*2];
            vector[index*2+1] = src.vector[inputIndex*2+1];
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
        // isRepeating should be false and src should be an instance of DecimalColumnVector.
        // However, we do not check these for performance considerations.
        LongDecimalColumnVector source = (LongDecimalColumnVector) src;

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
                this.vector[thisIndex*2] = source.vector[srcIndex*2];
                this.vector[thisIndex*2+1] = source.vector[srcIndex*2+1];
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
        long hash;
        for (int i = 0; i < hashCode.length; ++i)
        {
            if (this.isNull[i])
            {
                continue;
            }
            // FNV-1a style hash
            hash = 0x9E3779B185EBCA87L;
            hash = (hash ^ this.vector[i*2]) * 0xC2B2AE3D27D4EB4FL;
            hash = (hash ^ this.vector[i*2+1]) * 0xC2B2AE3D27D4EB4FL;
            hashCode[i] = 524287 * hashCode[i] + Long.hashCode(hash);
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int index, int otherIndex, ColumnVector other)
    {
        LongDecimalColumnVector otherVector = (LongDecimalColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return this.vector[index*2] == otherVector.vector[otherIndex*2] &&
                    this.vector[index*2+1] == otherVector.vector[otherIndex*2+1] &&
                    this.scale == otherVector.scale;
            // We assume the values never overflow and do not check the precisions.
        }
        return false;
    }

    @Override
    public int compareElement(int index, int otherIndex, ColumnVector other)
    {
        LongDecimalColumnVector otherVector = (LongDecimalColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            if (this.vector[index*2] != otherVector.vector[otherIndex*2])
            {
                return Long.compare(this.vector[index*2], otherVector.vector[otherIndex*2]);
            }
            if (this.vector[index*2+1] != otherVector.vector[otherIndex*2+1])
            {
                return Long.compare(this.vector[index*2+1], otherVector.vector[otherIndex*2+1]);
            }
            return Integer.compare(this.scale, otherVector.scale);
            // We assume the values never overflow and do not check the precisions.
        }
        return this.isNull[index] ? -1 : 1;
    }


    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof LongDecimalColumnVector)
        {
            LongDecimalColumnVector srcVector = (LongDecimalColumnVector) inputVector;
            this.vector = srcVector.vector;
            this.isNull = srcVector.isNull;
            this.writeIndex = srcVector.writeIndex;
            this.noNulls = srcVector.noNulls;
            this.isRepeating = srcVector.isRepeating;
            this.precision = srcVector.precision;
            this.scale = srcVector.scale;
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
                this.vector[j*2] = this.vector[i*2];
                this.vector[j*2+1] = this.vector[i*2+1];
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
            BigDecimal value;
            if (precision <= 18)
            {
                value = BigDecimal.valueOf(vector[row*2+1], scale);
            }
            else
            {
                BigInteger unscaledValue = new BigInteger(
                        Integer128.toBigEndianBytes(vector[row*2], vector[row*2+1]));
                value = new BigDecimal(unscaledValue, scale);
            }
            buffer.append(value);
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
        Arrays.fill(vector, DEFAULT_UNSCALED_VALUE);
    }

    @Override
    public void ensureSize(int size, boolean preserveData)
    {
        super.ensureSize(size, preserveData);
        if (size > vector.length)
        {
            long[] oldArray = vector;
            vector = new long[size*2];
            Arrays.fill(vector, DEFAULT_UNSCALED_VALUE);
            memoryUsage += (long) Long.BYTES * size * 2;
            length = size;
            if (preserveData)
            {
                if (isRepeating)
                {
                    vector[0] = oldArray[0];
                    vector[1] = oldArray[1];
                }
                else
                {
                    System.arraycopy(oldArray, 0, vector, 0, oldArray.length);
                }
            }
        }
    }
}
