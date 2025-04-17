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

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.utils.flat.ColumnVectorFlat;
import io.pixelsdb.pixels.core.utils.flat.VectorizedRowBatchFlat;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * VectorizedRowBatch derived from org.apache.hadoop.hive.ql.exec.vector
 * <p>
 * A VectorizedRowBatch is a set of rows, organized with each column
 * as a vector. It is the unit of query execution, organized to minimize
 * the cost per row and achieve high cycles-per-instruction.
 * The major fields are public by design to allow fast and convenient
 * access by the vectorized query execution code.
 */
public class VectorizedRowBatch implements AutoCloseable
{
    public int numCols;           // number of columns
    public ColumnVector[] cols;   // a vector for each column
    public int size;              // number of rows that qualify, i.e., haven't been filtered out.
    public int projectionSize;
    public int maxSize;           // capacity, i.e., the maximum number of rows can be stored in this row batch.

    private long memoryUsage = 0L;

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
        this.size = 0;
        this.maxSize = size;
        this.cols = new ColumnVector[numCols];

        memoryUsage += (long) Integer.BYTES * (size + numCols) +
        Integer.BYTES * 6 + Long.BYTES + 2;

        // Initially all columns are projected and in the same order
        projectionSize = numCols;
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
    public int count()
    {
        return size;
    }

    /**
     * Whether this row batch is empty, i.e., contains no data.
     *
     * @return true if this row batch is empty
     */
    public boolean isEmpty()
    {
        return this.size == 0;
    }

    /**
     * Whether this row batch is full, i.e., has no free space.
     *
     * @return true if this row batch is full
     */
    public boolean isFull()
    {
        return this.size >= this.maxSize;
    }

    /**
     * @return the number of remaining slots in this row batch.
     */
    public int freeSlots()
    {
        return maxSize - size;
    }

    /**
     * Add the selected elements in the src row batch into this row batch.
     *
     * @param selected the index of the selected elements in src
     * @param offset the start offset in selected
     * @param length the length in selected
     * @param src the source row batch
     */
    public void addSelected(int[] selected, int offset, int length, VectorizedRowBatch src)
    {
        checkArgument(offset >= 0 && length > 0,
                "invalid offset(?) or length(?)", offset, length);
        checkArgument(size + length <= maxSize,
                "too many selected rows (?)", length);
        for (int i = 0; i < cols.length; ++i)
        {
            cols[i].addSelected(selected, offset, length, src.cols[i]);
        }
        size += length;
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

        for (int i = 0; i < size; i++)
        {
            b.append('[');
            for (int k = 0; k < projectionSize; k++)
            {
                ColumnVector cv = cols[k];
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
        return b.toString();
    }

    public VectorizedRowBatch applyFilter(Bitmap filter)
    {
        requireNonNull(filter, "filter is null");
        checkArgument(filter.capacity() >= this.size,
                "filter is too small, filter capacity (" +
                        filter.capacity() + "), row batch size (" + this.size + ").");

        int cardinality = filter.cardinality(0, this.size);
        if (cardinality == this.size)
        {
            return this;
        }

        for (ColumnVector columnVector : this.cols)
        {
            columnVector.applyFilter(filter, this.size);
        }
        this.size = cardinality;

        return this;
    }

    /**
     * Project the column vectors from this row batch.
     * <b>Note: </b> the returned row batch should not be reused by the PixelsRecordReader.
     * @param projection indicates whether the column vector should be reserved,
     *                   its length is the same as this.numCols and this.projectionSize
     * @param projectionSize the number of true values, i.e., columns to reserve
     * @return this row batch after projection.
     */
    public VectorizedRowBatch applyProjection(boolean[] projection, int projectionSize)
    {
        ColumnVector[] newCols = new ColumnVector[projectionSize];
        for (int i = 0, j = 0; i < this.cols.length; ++i)
        {
            if (projection[i])
            {
                newCols[j++] = this.cols[i];
            }
            // does not reset the purged column vector for performance considerations.
        }
        this.cols = newCols;
        this.projectionSize = projectionSize;
        this.numCols = projectionSize;

        return this;
    }

    /**
     * Resets the row batch to default state for the next read.
     * - sets selectedInUse to false
     * - sets size to 0
     * - sets endOfFile to false
     * - resets each column
     * - inits each column
     */
    public void reset()
    {
        size = 0;
        endOfFile = false;
        for (ColumnVector vc : cols)
        {
            if (vc != null)
            {
                vc.reset();
                // Issue #367: No need to init after reset.
                // vc.init();
            }
        }
    }

    /**
     * Set the maximum number of rows in the batch.
     */
    public void ensureSize(int rows, boolean preserveData)
    {
        for (int i = 0; i < cols.length; ++i)
        {
            if (!cols[i].duplicated)
            {
                cols[i].ensureSize(rows, preserveData);
            }
        }
        if (!preserveData)
        {
            this.size = 0;
        }
        this.maxSize = rows;
    }

    /**
     * Get the approximate (maybe slightly lower than actual)
     * cumulative memory usage, which is more meaningful for GC
     * performance tuning.
     *
     * <br/>
     * <b>NOTE:</b> The memory usage of the column vectors are included.
     * @return
     */
    public long getMemoryUsage()
    {
        long colsUsage = 0;
        for (ColumnVector col : this.cols)
        {
            if (col != null)
            {
                colsUsage += col.getMemoryUsage();
            }
        }
        return memoryUsage + colsUsage;
    }

    @Override
    public void close()
    {
        size = 0;
        endOfFile = false;
        if (this.cols != null)
        {
            for (ColumnVector vc : cols)
            {
                if (vc != null)
                {
                    vc.close();
                }
            }
            this.cols = null;
        }
    }

    /**
     * Serialize VectorizedRowBatch to byte array
     * @return
     */
    public byte[] serialize(FlatBufferBuilder builder)
    {
        int[] columnVectorOffsets = new int[numCols];
        for (int i = 0; i < numCols; ++i)
        {
            columnVectorOffsets[i] = cols[i].serialize(builder);
        }
        int colsOffset = VectorizedRowBatchFlat.createColsVector(builder, columnVectorOffsets);

        VectorizedRowBatchFlat.startVectorizedRowBatchFlat(builder);
        VectorizedRowBatchFlat.addNumCols(builder, numCols);
        VectorizedRowBatchFlat.addCols(builder, colsOffset);
        VectorizedRowBatchFlat.addSize(builder, size);
        VectorizedRowBatchFlat.addProjectionSize(builder, projectionSize);
        VectorizedRowBatchFlat.addMaxSize(builder, maxSize);
        VectorizedRowBatchFlat.addMemoryUsage(builder, memoryUsage);
        VectorizedRowBatchFlat.addEndOfFile(builder, endOfFile);
        int batchOffset = VectorizedRowBatchFlat.endVectorizedRowBatchFlat(builder);

        builder.finish(batchOffset);
        return builder.sizedByteArray();
    }

    public static VectorizedRowBatch deserialize(byte[] data)
    {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        VectorizedRowBatchFlat batchFlat = VectorizedRowBatchFlat.getRootAsVectorizedRowBatchFlat(buffer);

        VectorizedRowBatch batch = new VectorizedRowBatch(batchFlat.numCols());
        batch.size = batchFlat.size();
        batch.projectionSize = batchFlat.projectionSize();
        batch.maxSize = batchFlat.maxSize();
        batch.memoryUsage = batchFlat.memoryUsage();
        batch.endOfFile = batchFlat.endOfFile();

        for (int i = 0; i < batchFlat.numCols(); ++i)
        {
            byte colType = batchFlat.colsType(i);
            Table colTable = new Table();
            batchFlat.cols(colTable, i);
            batch.cols[i] = ColumnVector.deserialize(colType, colTable);
        }
        return batch;
    }
}
