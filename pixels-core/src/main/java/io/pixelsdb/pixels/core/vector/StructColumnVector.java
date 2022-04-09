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
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void setElement(int outElementNum, int inputElementNum,
                           ColumnVector inputVector)
    {
        if (inputVector.isRepeating)
        {
            inputElementNum = 0;
        }
        if (inputVector.noNulls || !inputVector.isNull[inputElementNum])
        {
            isNull[outElementNum] = false;
            ColumnVector[] inputFields = ((StructColumnVector) inputVector).fields;
            for (int i = 0; i < inputFields.length; ++i)
            {
                fields[i].setElement(outElementNum, inputElementNum, inputFields[i]);
            }
        }
        else
        {
            noNulls = false;
            isNull[outElementNum] = true;
        }
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
    protected void applyFilter(Bitmap filter, int beforeIndex)
    {
        throw new UnsupportedOperationException("filter is not supported on StructColumnVector.");
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

    @Override
    public void setRepeating(boolean isRepeating)
    {
        super.setRepeating(isRepeating);
        for (int i = 0; i < fields.length; ++i)
        {
            fields[i].setRepeating(isRepeating);
        }
    }
}
