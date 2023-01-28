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

import io.pixelsdb.pixels.core.utils.Bitmap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.JvmUtils.unsafe;
import static io.pixelsdb.pixels.core.utils.BitUtils.longBytesToLong;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

/**
 * The dictionary encoded column vector for string or binary based data types.
 * This column vector is only used for reading string or binary data without decoding,
 * it does not implement the writing related methods.
 *
 * Created at: 28/01/2023
 * Author: hank
 */
public class DictionaryColumnVector extends ColumnVector
{
    // The backing array of the dictionary. If dictArray is null, it means dictionary is not set (initialized).
    public byte[] dictArray;

    /* The start offset of each value in the dictionary. The last element is the length of the dictionary.
     * We currently do not support dictionary larger than 2GB, thus using int (not long) array for start.
     */
    public int[] dictOffsets;

    /* The index of each data element in the dictionary.
     * E.g., there are 1000 data items in this column vector and 10 values in the dictionary, then ids has
     * 1000 elements, each in the range of 0 - 9, representing an index in the dictionary.
     */
    public int[] ids;

    /**
     * Use this constructor for normal operation.
     * All column vectors should be the default size normally.
     */
    public DictionaryColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    /**
     * Don't call this constructor except for testing purposes.
     *
     * @param size number of elements in the column vector
     */
    public DictionaryColumnVector(int size)
    {
        super(size);
        ids = new int[size];
        memoryUsage += (long) Integer.BYTES * size;
    }

    /**
     * Additional reset work for BinaryColumnVector (releasing scratch bytes for by value strings).
     * Reset must be called before reading the next row batch.
     */
    @Override
    public void reset()
    {
        super.reset();
        this.dictArray = null;
        this.dictOffsets = null;
        // no need to clear this.ids
    }

    /**
     * For performance considerations, we do not check if the dictionary is initialized or contains
     * enough values. This much be ensured by the user of this method.
     * @param id the id in the dictionary.
     */
    @Override
    public void add(int id)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        ids[writeIndex++] = id;
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
            int start = this.dictOffsets[this.ids[i]],  len = this.dictOffsets[this.ids[i] + 1] - start;
            int h = 1;
            long address = ARRAY_BYTE_BASE_OFFSET + start, word;
            while (len >= Long.BYTES)
            {
                word = unsafe.getLong(this.dictArray, address);
                h = 31 * h + (int) (word ^ word >>> 32);
                address += Long.BYTES;
                len -= Long.BYTES;
            }
            int offset = (int) (address - ARRAY_BYTE_BASE_OFFSET);
            while (len-- > 0)
            {
                h = h * 31 + (int) this.dictArray[offset++];
            }
            hashCode[i] = 31 * hashCode[i] + h;
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int thisIndex, int thatIndex, ColumnVector thatVector)
    {
        DictionaryColumnVector that = (DictionaryColumnVector) thatVector;
        if (!this.isNull[thisIndex] && !that.isNull[thatIndex])
        {
            int thisStart = this.dictOffsets[this.ids[thisIndex]];
            int thatStart = that.dictOffsets[that.ids[thatIndex]];
            int thisLen = this.dictOffsets[this.ids[thisIndex] + 1] - thisStart;
            int thatLen = that.dictOffsets[that.ids[thatIndex] + 1] - thatStart;
            if (thisLen != thatLen)
            {
                return false;
            }

            if (this.dictArray == that.dictArray && thisStart == thatStart && thisLen == thatLen)
            {
                return true;
            }

            int compareLen = thisLen; // thisLen == thatLen
            long thisAddress = ARRAY_BYTE_BASE_OFFSET + thisStart;
            long thatAddress = ARRAY_BYTE_BASE_OFFSET + thatStart;
            long thisWord, thatWord;

            while (compareLen >= Long.BYTES)
            {
                thisWord = unsafe.getLong(this.dictArray, thisAddress);
                thatWord = unsafe.getLong(that.dictArray, thatAddress);
                if (thisWord != thatWord)
                {
                    return false;
                }
                thisAddress += Long.BYTES;
                thatAddress += Long.BYTES;
                compareLen -= Long.BYTES;
            }

            thisStart = (int) (thisAddress - ARRAY_BYTE_BASE_OFFSET);
            thatStart = (int) (thatAddress - ARRAY_BYTE_BASE_OFFSET);

            while (compareLen-- > 0)
            {
                if (this.dictArray[thisStart++] != that.dictArray[thatStart++])
                {
                    return false;
                }
            }

            return true;
        }
        return false;
    }

    @Override
    public int compareElement(int thisIndex, int thatIndex, ColumnVector thatVector)
    {
        DictionaryColumnVector that = (DictionaryColumnVector) thatVector;
        if (!this.isNull[thisIndex] && !that.isNull[thatIndex])
        {
            int thisStart = this.dictOffsets[this.ids[thisIndex]];
            int thatStart = that.dictOffsets[that.ids[thatIndex]];
            int thisLen = this.dictOffsets[this.ids[thisIndex] + 1] - thisStart;
            int thatLen = that.dictOffsets[that.ids[thatIndex] + 1] - thatStart;
            if (this.dictArray == that.dictArray && thisStart == thatStart && thisLen == thatLen)
            {
                return 0;
            }

            int compareLen = Math.min(thisLen, thatLen);
            long thisAddress = ARRAY_BYTE_BASE_OFFSET + thisStart;
            long thatAddress = ARRAY_BYTE_BASE_OFFSET + thatStart;
            long thisWord, thatWord;

            while (compareLen >= Long.BYTES)
            {
                thisWord = unsafe.getLong(this.dictArray, thisAddress);
                thatWord = unsafe.getLong(that.dictArray, thatAddress);
                if (thisWord != thatWord)
                {
                    return longBytesToLong(thisWord) < longBytesToLong(thatWord) ? -1 : 1;
                }
                thisAddress += Long.BYTES;
                thatAddress += Long.BYTES;
                compareLen -= Long.BYTES;
            }

            thisStart = (int) (thisAddress - ARRAY_BYTE_BASE_OFFSET);
            thatStart = (int) (thatAddress - ARRAY_BYTE_BASE_OFFSET);

            int c;
            while (compareLen-- > 0)
            {
                c = (this.dictArray[thisStart++] & 0xFF) - (that.dictArray[thatStart++] & 0xFF);
                if (c != 0)
                {
                    return c;
                }
            }

            return Integer.compare(thisLen, thatLen);
        }
        return this.isNull[thisIndex] ? -1 : 1;
    }

    /**
     * Simplify vector by brute-force flattening noNulls and isRepeating
     * This can be used to reduce combinatorial explosion of code paths in VectorExpressions
     * with many arguments, at the expense of loss of some performance.
     */
    public void flatten(boolean selectedInUse, int[] sel, int size)
    {
        throw new UnsupportedOperationException("flatten is not supported.");
    }

    @Override
    public void addElement(int inputIndex, ColumnVector inputVector)
    {
        throw new UnsupportedOperationException("add is not supported.");
    }

    @Override
    public void addSelected(int[] selected, int offset, int length, ColumnVector src)
    {
        throw new UnsupportedOperationException("add is not supported.");
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        throw new UnsupportedOperationException("duplicate is not supported.");
    }

    @Override
    public void init()
    {
        // initBuffer(0);
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
                this.ids[j] = this.ids[i];
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

    public String toString(int row)
    {
        if (isRepeating)
        {
            row = 0;
        }
        if (noNulls || !isNull[row])
        {
            int id = ids[row];
            return new String(dictArray, dictOffsets[id], dictOffsets[id+1] - dictOffsets[id]);
        }
        else
        {
            return null;
        }
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
            buffer.append('"');
            int id = ids[row];
            buffer.append(new String(dictArray, dictOffsets[id], dictOffsets[id+1] - dictOffsets[id]));
            buffer.append('"');
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
        if (size > ids.length)
        {
            int[] oldIds = ids;
            ids = new int[size];
            memoryUsage += Integer.BYTES * size * 2;
            length = size;
            if (preserveData)
            {
                System.arraycopy(oldIds, 0, ids, 0, oldIds.length);
            }
        }
    }

    @Override
    public void close()
    {
        super.close();
        this.dictArray = null;
        this.dictOffsets = null;
        this.ids = null;
    }
}
