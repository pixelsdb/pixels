package cn.edu.ruc.iir.pixels.core.vector;

/**
 * VectorizedRowBatch from org.apache.hadoop.hive.ql.exec.vector
 * <p>
 * A VectorizedRowBatch is a set of rows, organized with each column
 * as a vector. It is the unit of query execution, organized to minimize
 * the cost per row and achieve high cycles-per-instruction.
 * The major fields are public by design to allow fast and convenient
 * access by the vectorized query execution code.
 */
public class VectorizedRowBatch
{
    public int numCols;           // number of columns
    public ColumnVector[] cols;   // a vector for each column
    public int size;              // number of rows that qualify (i.e. haven't been filtered out)
    public int[] selected;        // array of positions of selected values
    public int[] projectedColumns;
    public int projectionSize;
    public int maxSize;

    private int dataColumnCount;
    private int partitionColumnCount;

    /*
     * If no filtering has been applied yet, selectedInUse is false,
     * meaning that all rows qualify. If it is true, then the selected[] array
     * records the offsets of qualifying rows.
     */
    public boolean selectedInUse;

    // If this is true, then there is no data in the batch -- we have hit the end of input.
    public boolean endOfFile;

    /*
     * This number is carefully chosen to minimize overhead and typically allows
     * one VectorizedRowBatch to fit in cache.
     */
    public static final int DEFAULT_SIZE = 1024;

    /**
     * Return a batch with the specified number of columns.
     * This is the standard constructor -- all batches should be the same size
     *
     * @param numCols the number of columns to include in the batch
     */
    public VectorizedRowBatch(int numCols)
    {
        this(numCols, DEFAULT_SIZE);
    }

    /**
     * Return a batch with the specified number of columns and rows.
     * Only call this constructor directly for testing purposes.
     * Batch size should normally always be defaultSize.
     *
     * @param numCols the number of columns to include in the batch
     * @param size    the number of rows to include in the batch
     */
    public VectorizedRowBatch(int numCols, int size)
    {
        this.numCols = numCols;
        this.size = size;
        this.maxSize = size;
        selected = new int[size];
        selectedInUse = false;
        this.cols = new ColumnVector[numCols];
        projectedColumns = new int[numCols];

        // Initially all columns are projected and in the same order
        projectionSize = numCols;
        for (int i = 0; i < numCols; i++)
        {
            projectedColumns[i] = i;
        }

        dataColumnCount = -1;
        partitionColumnCount = -1;
    }

    public void setPartitionInfo(int dataColumnCount, int partitionColumnCount)
    {
        this.dataColumnCount = dataColumnCount;
        this.partitionColumnCount = partitionColumnCount;
    }

    public int getDataColumnCount()
    {
        return dataColumnCount;
    }

    public int getPartitionColumnCount()
    {
        return partitionColumnCount;
    }

    /**
     * Returns the maximum size of the batch (number of rows it can hold)
     */
    public int getMaxSize()
    {
        return maxSize;
    }

    /**
     * Return count of qualifying rows.
     *
     * @return number of rows that have not been filtered out
     */
    public long count()
    {
        return size;
    }

    private static String toUTF8(Object o)
    {
        if (o == null)
        {
            return "\\N"; /* as found in LazySimpleSerDe's nullSequence */
        }
        return o.toString();
    }

    @Override
    public String toString()
    {
        if (size == 0)
        {
            return "";
        }
        StringBuilder b = new StringBuilder();
        if (this.selectedInUse)
        {
            for (int j = 0; j < size; j++)
            {
                int i = selected[j];
                b.append('[');
                for (int k = 0; k < projectionSize; k++)
                {
                    int projIndex = projectedColumns[k];
                    ColumnVector cv = cols[projIndex];
                    if (k > 0)
                    {
                        b.append(", ");
                    }
                    cv.stringifyValue(b, i);
                }
                b.append(']');
                if (j < size - 1)
                {
                    b.append('\n');
                }
            }
        }
        else
        {
            for (int i = 0; i < size; i++)
            {
                b.append('[');
                for (int k = 0; k < projectionSize; k++)
                {
                    int projIndex = projectedColumns[k];
                    ColumnVector cv = cols[projIndex];
                    if (k > 0)
                    {
                        b.append(", ");
                    }
                    if (cv != null)
                    {
                        cv.stringifyValue(b, i);
                    }
                }
                b.append(']');
                if (i < size - 1)
                {
                    b.append('\n');
                }
            }
        }
        return b.toString();
    }

    /**
     * Resets the row batch to default state
     * - sets selectedInUse to false
     * - sets size to 0
     * - sets endOfFile to false
     * - resets each column
     * - inits each column
     */
    public void reset()
    {
        selectedInUse = false;
        size = 0;
        endOfFile = false;
        for (ColumnVector vc : cols)
        {
            if (vc != null)
            {
                vc.reset();
                vc.init();
            }
        }
    }

    /**
     * Set the maximum number of rows in the batch.
     * Data is not preserved.
     */
    public void ensureSize(int rows)
    {
        for (int i = 0; i < cols.length; ++i)
        {
            if (!cols[i].duplicated)
            {
                cols[i].ensureSize(rows, false);
            }
        }
    }
}
