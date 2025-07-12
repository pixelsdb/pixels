package io.pixelsdb.pixels.core.vector;

import com.google.flatbuffers.FlatBufferBuilder;
import io.pixelsdb.pixels.core.flat.ColumnVectorFlat;
import io.pixelsdb.pixels.core.flat.DoubleArray;
import io.pixelsdb.pixels.core.flat.VectorColumnVectorFlat;
import io.pixelsdb.pixels.core.utils.Bitmap;

import java.nio.DoubleBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.JvmUtils.*;
import static java.util.Objects.requireNonNull;

public class VectorColumnVector extends ColumnVector
{
    // a vector of vectors. todo maybe worth try using long instead for better cpu performance because  some cpus don't handle float computations well
    public double[][] vector;

    // dimension of vectors in this column todo enforce this in schema
    public int dimension;

    public VectorColumnVector(int len, int dimension)
    {
        super(len);
        this.dimension = dimension;
        vector = new double[len][dimension];
        Arrays.fill(vector, null);
        memoryUsage += (long)Double.BYTES * dimension * len;
    }

    @Override
    public void addElement(int inputIndex, ColumnVector inputVector)
    {
        int index = writeIndex++;
        if (inputVector.noNulls || !inputVector.isNull[inputIndex])
        {
            // we are not adding a null value
            isNull[index] = false;
            VectorColumnVector in = (VectorColumnVector) inputVector;
            setRef(index, in.vector[inputIndex]);
        }
        else
        {
            isNull[index] = true;
            noNulls = false;
        }
    }

    /**
     * Set a field by reference.
     *
     * @param elementNum index within column vector to set
     * @param sourceBuf  container of source data
     */
    public void setRef(int elementNum, double[] sourceBuf)
    {
        if (elementNum >= writeIndex)
        {
            writeIndex = elementNum + 1;
        }
        this.vector[elementNum] = sourceBuf;
        this.isNull[elementNum] = sourceBuf == null;
        if (sourceBuf == null)
        {
            this.noNulls = false;
        }
    }

    @Override
    public void addSelected(int[] selected, int offset, int length, ColumnVector src)
    {
        VectorColumnVector source = (VectorColumnVector) src;

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
                // We do not change the content of the elements in the vector, thus it is safe to setRef.
                this.setRef(thisIndex, source.vector[srcIndex]);
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
            // todo there's probably a better implementation, but should function fine for now
            hashCode[i] = 31 * hashCode[i] + (int)(Double.doubleToRawLongBits(this.vector[i][0]) ^ (Double.doubleToRawLongBits(this.vector[i][0]) >>> 16));
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int index, int otherIndex, ColumnVector other)
    {
        VectorColumnVector otherVector = (VectorColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return Arrays.equals(this.vector[index], otherVector.vector[otherIndex]);
        }
        return false;
    }

    @Override
    public int compareElement(int index, int otherIndex, ColumnVector other)
    {
        VectorColumnVector otherVector = (VectorColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            return compare(this.vector[index], otherVector.vector[otherIndex]);
        }
        return this.isNull[index] ? -1 : 1;
    }

    /**
     * compare two double arrays lexicographically. copied from newer version of standard java library
     * @return <0,0,>0 for <,=,> respectively
     */
    private static int compare(double[] a, double[] b)
    {
        if (a == b)
        {
            return 0;
        }
        if (a == null || b == null)
        {
            return a == null ? -1 : 1;
        }
        int i = mismatch(a, b, Math.min(a.length, b.length));
        if (i >= 0)
        {
            return Double.compare(a[i], b[i]);
        }
        return a.length - b.length;
    }

    @Override
    public void flatten(boolean selectedInUse, int[] sel, int size)
    {
        flattenPush();
        if (isRepeating)
        {
            isRepeating = false;
            // expand and fill vector with vector[0]
            if (noNulls || !isNull[0])
            {
                // loops start at position 1 because position 0 is already set
                if (selectedInUse)
                {
                    for (int j = 1; j < size; j++)
                    {
                        int i = sel[j];
                        this.setRef(i, vector[0]);
                    }
                }
                else
                {
                    for (int i = 1; i < size; i++)
                    {
                        this.setRef(i, vector[0]);
                    }
                }
            }
            flattenRepeatingNulls(selectedInUse, sel, size);
        }
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof VectorColumnVector)
        {
            VectorColumnVector srcVector = (VectorColumnVector) inputVector;
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
            buffer.append(Arrays.toString(vector[row]));
        }
        else
        {
            buffer.append("null");
        }
    }

    @Override
    public void add(double[] vec)
    {
        assert(vec.length == dimension);
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        int index = writeIndex++;
        vector[index] = vec;
        isNull[index] = false;
    }

    @Override
    public byte getFlatBufferType()
    {
        return ColumnVectorFlat.VectorColumnVectorFlat;
    }

    @Override
    public int serialize(FlatBufferBuilder builder)
    {
        int baseOffset = super.serialize(builder);
        int[] doubleArrayOffsets = new int[writeIndex];
        for (int i = 0; i < writeIndex; ++i)
        {
            int doublesOffset = DoubleArray.createDoublesVector(builder, (vector[i] == null) ? new double[0] : vector[i]);
            DoubleArray.startDoubleArray(builder);
            DoubleArray.addDoubles(builder, doublesOffset);
            doubleArrayOffsets[i] = DoubleArray.endDoubleArray(builder);
        }
        int vectorVectorOffset = VectorColumnVectorFlat.createVectorVector(builder, doubleArrayOffsets);
        VectorColumnVectorFlat.startVectorColumnVectorFlat(builder);
        VectorColumnVectorFlat.addBase(builder, baseOffset);
        VectorColumnVectorFlat.addVector(builder, vectorVectorOffset);
        VectorColumnVectorFlat.addDimension(builder, dimension);
        return VectorColumnVectorFlat.endVectorColumnVectorFlat(builder);
    }

    public static double[] getDoublesFromFlatArray(DoubleArray doubleArray)
    {
        DoubleBuffer dbuf = doubleArray.doublesAsByteBuffer().asDoubleBuffer();
        double[] result = new double[dbuf.remaining()];
        dbuf.get(result);
        return result;
    }

    public static VectorColumnVector deserialize(VectorColumnVectorFlat flat)
    {
        VectorColumnVector vector = new VectorColumnVector(flat.base().length(), flat.dimension());
        vector.deserializeBase(flat.base());
        for (int i = 0; i < vector.writeIndex; ++i)
        {
            DoubleArray doubleArray = flat.vector(i);
            vector.vector[i] = vector.isNull[i] ? null : getDoublesFromFlatArray(doubleArray);
        }
        return vector;
    }
}
