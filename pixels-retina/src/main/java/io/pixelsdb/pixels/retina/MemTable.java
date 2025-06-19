/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.retina;

import static com.google.common.base.Preconditions.checkArgument;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

public class MemTable implements Referenceable
{
    private final ReferenceCounter refCounter = new ReferenceCounter();
    private final long id;  // unique identifier
    private final TypeDescription schema;
    private final VectorizedRowBatch rowBatch;
    private long size;

    public MemTable(long id, TypeDescription schema, int pixelStride, int mode)
    {
        this.id = id;
        this.schema = schema;
        this.rowBatch = schema.createRowBatchWithHiddenColumn(pixelStride, mode);
        this.size = 0L;

        // init reference count
        this.refCounter.ref();
    }

    /**
     * values is one record with all column values and timestamp.
     * return the rowId after successfully inserting data into the index;
     * return -1 on failure.
     * @param values
     * @param timestamp
     * @return
     * @throws RetinaException
     */
    public synchronized long add(byte[][] values, long timestamp, RecordLocation recordLocation) throws RetinaException
    {
        if (isFull())
        {
            return -1L;
        }
        int columnCount = schema.getChildren().size();
        checkArgument(values.length == columnCount,
                "Column values count does not match schema column count");

        recordLocation.setLocationIdentifier(this.id);
        recordLocation.setRecordIndex(this.rowBatch.size);

        for (int i = 0; i < values.length; ++i)
        {
            this.rowBatch.cols[i].add(new String(values[i]));
            this.size += values[i].length;
        }
        this.rowBatch.cols[columnCount].add(timestamp);
        this.size += 8;
        this.rowBatch.size++;

        // insert the record into index
        long rowId = 0L;
        return rowId;
    }

    public long getId()
    {
        return id;
    }

    public VectorizedRowBatch getRowBatch()
    {
        return this.rowBatch;
    }

    public long getSize()
    {
        return this.size;
    }

    public boolean isFull()
    {
        return this.rowBatch.isFull();
    }

    public boolean isEmpty()
    {
        return this.rowBatch.isEmpty();
    }

    public byte[] serialize()
    {
        return this.rowBatch.serialize();
    }

    @Override
    public void ref()
    {
        this.refCounter.ref();
    }

    @Override
    public boolean unref()
    {
        boolean shouldDelete = this.refCounter.unref();
        if (shouldDelete)
        {
            if (this.rowBatch != null)
            {
                this.rowBatch.close();
            }
        }
        return shouldDelete;
    }

    @Override
    public int getRefCount()
    {
        return this.refCounter.getCount();
    }
}
