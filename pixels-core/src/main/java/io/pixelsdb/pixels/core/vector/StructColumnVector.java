package io.pixelsdb.pixels.core.vector;

/**
 * StructColumnVector from org.apache.hadoop.hive.ql.exec.vector
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
    public void init()
    {
        super.init();
        for (int i = 0; i < fields.length; ++i)
        {
            fields[i].init();
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
