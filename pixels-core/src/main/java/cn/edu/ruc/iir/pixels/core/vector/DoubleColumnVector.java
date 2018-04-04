package cn.edu.ruc.iir.pixels.core.vector;

import java.util.Arrays;

/**
 * DoubleColumnVector from org.apache.hadoop.hive.ql.exec.vector
 *
 * This class represents a nullable double precision floating point column vector.
 * This class will be used for operations on all floating point types (float, double)
 * and as such will use a 64-bit double value to hold the biggest possible value.
 * During copy-in/copy-out, smaller types (i.e. float) will be converted as needed. This will
 * reduce the amount of code that needs to be generated and also will run fast since the
 * machine operates with 64-bit words.
 *
 * The vector[] field is public by design for high-performance access in the inner
 * loop of query execution.
 */
public class DoubleColumnVector extends ColumnVector
{
    public double[] vector;
    public static final double NULL_VALUE = Double.NaN;

    /**
     * Use this constructor by default. All column vectors
     * should normally be the default size.
     */
    public DoubleColumnVector() {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    /**
     * Don't use this except for testing purposes.
     *
     * @param len
     */
    public DoubleColumnVector(int len) {
        super(len);
        vector = new double[len];
    }

    // Copy the current object contents into the output. Only copy selected entries,
    // as indicated by selectedInUse and the sel array.
    public void copySelected(
            boolean selectedInUse, int[] sel, int size, DoubleColumnVector output) {
        // Output has nulls if and only if input has nulls.
        output.noNulls = noNulls;
        output.isRepeating = false;

        // Handle repeating case
        if (isRepeating) {
            output.vector[0] = vector[0];
            output.isNull[0] = isNull[0];
            output.isRepeating = true;
            return;
        }

        // Handle normal case

        // Copy data values over
        if (selectedInUse) {
            for (int j = 0; j < size; j++) {
                int i = sel[j];
                output.vector[i] = vector[i];
            }
        }
        else {
            System.arraycopy(vector, 0, output.vector, 0, size);
        }

        // Copy nulls over if needed
        if (!noNulls) {
            if (selectedInUse) {
                for (int j = 0; j < size; j++) {
                    int i = sel[j];
                    output.isNull[i] = isNull[i];
                }
            }
            else {
                System.arraycopy(isNull, 0, output.isNull, 0, size);
            }
        }
    }

    // Fill the column vector with the provided value
    public void fill(double value) {
        noNulls = true;
        isRepeating = true;
        vector[0] = value;
    }

    // Fill the column vector with nulls
    public void fillWithNulls() {
        noNulls = false;
        isRepeating = true;
        vector[0] = NULL_VALUE;
        isNull[0] = true;
    }

    // Simplify vector by brute-force flattening noNulls and isRepeating
    // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
    // with many arguments.
    public void flatten(boolean selectedInUse, int[] sel, int size) {
        flattenPush();
        if (isRepeating) {
            isRepeating = false;
            double repeatVal = vector[0];
            if (selectedInUse) {
                for (int j = 0; j < size; j++) {
                    int i = sel[j];
                    vector[i] = repeatVal;
                }
            } else {
                Arrays.fill(vector, 0, size, repeatVal);
            }
            flattenRepeatingNulls(selectedInUse, sel, size);
        }
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void add(float value)
    {
        add((double) value);
    }

    @Override
    public void add(float[] values)
    {
        for (float v : values) {
            add(v);
        }
    }

    @Override
    public void add(double value)
    {
        if (writeIndex >= getLength()) {
            ensureSize(writeIndex * 2, true);
        }
        vector[writeIndex++] = value;
    }

    @Override
    public void add(double[] values)
    {
        for (double v : values) {
            add(v);
        }
    }

    @Override
    public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {
        if (inputVector.isRepeating) {
            inputElementNum = 0;
        }
        if (inputVector.noNulls || !inputVector.isNull[inputElementNum]) {
            isNull[outElementNum] = false;
            vector[outElementNum] =
                    ((DoubleColumnVector) inputVector).vector[inputElementNum];
        } else {
            isNull[outElementNum] = true;
            noNulls = false;
        }
    }

    @Override
    public void stringifyValue(StringBuilder buffer, int row) {
        if (isRepeating) {
            row = 0;
        }
        if (noNulls || !isNull[row]) {
            buffer.append(vector[row]);
        } else {
            buffer.append("null");
        }
    }

    @Override
    public void ensureSize(int size, boolean preserveData) {
        super.ensureSize(size, preserveData);
        if (size > vector.length) {
            double[] oldArray = vector;
            vector = new double[size];
            length = size;
            if (preserveData) {
                if (isRepeating) {
                    vector[0] = oldArray[0];
                } else {
                    System.arraycopy(oldArray, 0, vector, 0 , oldArray.length);
                }
            }
        }
    }
}
