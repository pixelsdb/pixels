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

import com.google.flatbuffers.FlatBufferBuilder;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.utils.flat.ColumnVectorFlat;
import io.pixelsdb.pixels.core.utils.flat.DecimalColumnVectorFlat;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.core.TypeDescription.MAX_SHORT_DECIMAL_PRECISION;
import static io.pixelsdb.pixels.core.TypeDescription.MAX_SHORT_DECIMAL_SCALE;
import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.util.Objects.requireNonNull;

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * @author hank
 * @create 2022-03-05
 */
public class DecimalColumnVector extends ColumnVector
{
    public static final long DEFAULT_UNSCALED_VALUE = 0;
    public long[] vector;
    private int precision;
    private int scale;

    public DecimalColumnVector(int precision, int scale)
    {
        this(VectorizedRowBatch.DEFAULT_SIZE, precision, scale);
    }

    public DecimalColumnVector(int len, int precision, int scale)
    {
        super(len);
        vector = new long[len];
        Arrays.fill(vector, DEFAULT_UNSCALED_VALUE);
        memoryUsage += Long.BYTES * len + Integer.BYTES * 2;

        if (precision < 1)
        {
            throw new IllegalArgumentException("precision " + precision + " is negative");
        }
        else if (precision > MAX_SHORT_DECIMAL_PRECISION)
        {
            throw new IllegalArgumentException("precision " + precision +
                    " is out of the max precision " + MAX_SHORT_DECIMAL_PRECISION);
        }
        this.precision = precision;

        if (scale < 0)
        {
            throw new IllegalArgumentException("scale " + scale + " is negative");
        }
        else if (scale > MAX_SHORT_DECIMAL_SCALE)
        {
            throw new IllegalArgumentException("scale " + scale +
                    " is out of the max scale " + MAX_SHORT_DECIMAL_SCALE);
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
        vector[0] = decimal.unscaledValue().longValueExact();
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
        // As we only support max precision 18, it is safe to convert unscaled value to long.
        vector[index] = decimal.unscaledValue().longValue();
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
        // As we only support max precision 18, it is safe to convert unscaled value to long.
        vector[index] = decimal.unscaledValue().longValue();
        isNull[index] = false;
    }

    @Override
    public void addElement(int inputIndex, ColumnVector inputVector)
    {
        int index = writeIndex++;
        if (inputVector.noNulls || !inputVector.isNull[inputIndex])
        {
            isNull[index] = false;
            vector[index] = ((DecimalColumnVector) inputVector).vector[inputIndex];
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
        DecimalColumnVector source = (DecimalColumnVector) src;

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
            hashCode[i] = 31 * hashCode[i] + (int)(this.vector[i] ^ (this.vector[i] >>> 16));
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int index, int otherIndex, ColumnVector other)
    {
        DecimalColumnVector otherVector = (DecimalColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return this.vector[index] == otherVector.vector[otherIndex] &&
                    this.scale == otherVector.scale;
            // We assume the values never overflow and do not check the precisions.
        }
        return false;
    }

    @Override
    public int compareElement(int index, int otherIndex, ColumnVector other)
    {
        DecimalColumnVector otherVector = (DecimalColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            if (this.vector[index] != otherVector.vector[otherIndex])
            {
                return Long.compare(this.vector[index],otherVector.vector[otherIndex]);
            }
            return Integer.compare(this.scale, otherVector.scale);
            // We assume the values never overflow and do not check the precisions.
        }
        return this.isNull[index] ? -1 : 1;
    }


    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof DecimalColumnVector)
        {
            DecimalColumnVector srcVector = (DecimalColumnVector) inputVector;
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
            BigDecimal value = BigDecimal.valueOf(vector[row], scale);
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
            vector = new long[size];
            Arrays.fill(vector, DEFAULT_UNSCALED_VALUE);
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
    public byte getFlatBufferType()
    {
        return ColumnVectorFlat.DecimalColumnVectorFlat;
    }

    @Override
    public int serialize(FlatBufferBuilder builder)
    {
        int baseOffset = super.serialize(builder);
        int vectorVectorOffset = DecimalColumnVectorFlat.createVectorVector(builder, vector);

        DecimalColumnVectorFlat.startDecimalColumnVectorFlat(builder);
        DecimalColumnVectorFlat.addBase(builder, baseOffset);
        DecimalColumnVectorFlat.addVector(builder, vectorVectorOffset);
        DecimalColumnVectorFlat.addPrecision(builder, precision);
        DecimalColumnVectorFlat.addScale(builder, scale);
        return DecimalColumnVectorFlat.endDecimalColumnVectorFlat(builder);
    }

    public static DecimalColumnVector deserialize(DecimalColumnVectorFlat flat)
    {
        DecimalColumnVector vector = new DecimalColumnVector(flat.base().length(), flat.precision(), flat.scale());
        for (int i = 0; i < flat.vectorLength(); ++i)
        {
            vector.vector[i] = flat.vector(i);
        }
        vector.deserializeBase(flat.base());
        return vector;
    }
}
