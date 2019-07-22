package cn.edu.ruc.iir.pixels.core.vector;

import java.util.Arrays;

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
    }

    @Override
    public void add(boolean value)
    {
        add(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public void add(byte value)
    {
        vector[writeIndex++] = value;
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
            flattenRepeatingNulls(selectedInUse, sel, size);
        }
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector)
    {
        if (inputVector.isRepeating)
        {
            inputElementNum = 0;
        }
        if (inputVector.noNulls || !inputVector.isNull[inputElementNum])
        {
            isNull[outElementNum] = false;
            vector[outElementNum] =
                    ((ByteColumnVector) inputVector).vector[inputElementNum];
        }
        else
        {
            isNull[outElementNum] = true;
            noNulls = false;
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
            byte[] oldArray = vector;
            vector = new byte[size];
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
}
