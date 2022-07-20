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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * StructColumnVector derived from org.apache.hadoop.hive.ql.exec.vector
 * <p>
 * The representation of a vectorized column of struct objects.
 * <p>
 * Each field is represented by a separate inner ColumnVector. Since this
 * ColumnVector doesn't own any per row data other that the isNull flag, the
 * isRepeating only covers the isNull array.
 */
public class StructColumnVector extends ColumnVector
{
    public ColumnVector[] fields;

    public StructColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    /**
     * Constructor for StructColumnVector
     *
     * @param len    Vector length
     * @param fields the field column vectors
     */
    public StructColumnVector(int len, ColumnVector... fields)
    {
        super(len);
        this.fields = fields;
    }

    @Override
    public void flatten(boolean selectedInUse, int[] sel, int size)
    {
        flattenPush();
        for (int i = 0; i < fields.length; ++i)
        {
            fields[i].flatten(selectedInUse, sel, size);
        }
        writeIndex = size;
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void addElement(int inputIndex, ColumnVector inputVector)
    {
        int index = writeIndex++;
        if (inputVector.noNulls || !inputVector.isNull[inputIndex])
        {
            isNull[index] = false;
            ColumnVector[] inputFields = ((StructColumnVector) inputVector).fields;
            for (int i = 0; i < inputFields.length; ++i)
            {
                fields[i].addElement(inputIndex, inputFields[i]);
            }
        }
        else
        {
            noNulls = false;
            isNull[index] = true;
        }
    }

    @Override
    public void addSelected(int[] selected, int offset, int length, ColumnVector src)
    {
        // isRepeating should be false and src should be an instance of StructColumnVector.
        // However, we do not check these for performance considerations.
        StructColumnVector source = (StructColumnVector) src;

        for (int i = 0; i < fields.length; i++)
        {
            this.fields[i].addSelected(selected, offset, length, source.fields[i]);
        }
        writeIndex += length;
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof StructColumnVector)
        {
            StructColumnVector srcVector = (StructColumnVector) inputVector;
            for (int i = 0; i < fields.length; i++)
            {
                fields[i].duplicate(srcVector.fields[i]);
            }
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
            buffer.append('[');
            for (int i = 0; i < fields.length; ++i)
            {
                if (i != 0)
                {
                    buffer.append(", ");
                }
                fields[i].stringifyValue(buffer, row);
            }
            buffer.append(']');
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
        for (int i = 0; i < fields.length; ++i)
        {
            fields[i].ensureSize(size, preserveData);
        }
    }

    @Override
    public int[] accumulateHashCode(int[] hashCode)
    {
        requireNonNull(hashCode, "hashCode is null");
        checkArgument(hashCode.length > 0 && hashCode.length <= this.length, "",
                "the length of hashCode is not in the range [1, length]");
        for (ColumnVector field : this.fields)
        {
            field.accumulateHashCode(hashCode);
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int index, int otherIndex, ColumnVector other)
    {
        StructColumnVector otherVector = (StructColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            for (int i = 0; i < this.fields.length; ++i)
            {
                if (!this.fields[i].elementEquals(index, otherIndex, otherVector.fields[i]))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int compareElement(int index, int otherIndex, ColumnVector other)
    {
        StructColumnVector otherVector = (StructColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            for (int i = 0; i < this.fields.length; ++i)
            {
                int c = this.fields[i].compareElement(index, otherIndex, otherVector.fields[i]);
                if (c != 0)
                {
                    return c;
                }
            }
            return 0;
        }
        return this.isNull[index] ? -1 : 1;
    }

    @Override
    public void reset()
    {
        super.reset();
        for (int i = 0; i < fields.length; ++i)
        {
            fields[i].reset();
        }
    }

    @Override
    public void close()
    {
        super.close();
        if (this.fields != null)
        {
            for (ColumnVector field : fields)
            {
                field.close();
            }
            this.fields = null;
        }
    }

    @Override
    public void init()
    {
        super.init();
        for (int i = 0; i < fields.length; ++i)
        {
            fields[i].init();
        }
    }

    @Override
    protected void applyFilter(Bitmap filter, int before)
    {
        for (ColumnVector column : this.fields)
        {
            column.applyFilter(filter, before);
        }
    }

    @Override
    public void unFlatten()
    {
        super.unFlatten();
        for (int i = 0; i < fields.length; ++i)
        {
            fields[i].unFlatten();
        }
    }
}
