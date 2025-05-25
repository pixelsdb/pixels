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
    private final TypeDescription schema;
    private final VectorizedRowBatch rowBatch;

    public MemTable(TypeDescription schema, int pixelStride, int mode)
    {
        this.schema = schema;
        this.rowBatch = schema.createRowBatchWithHiddenColumn(pixelStride, mode);

        // init reference count
        this.refCounter.ref();
    }

    /**
     * values is one record with all column values and timestamp.
     * @param values
     * @param timestamp
     * @return
     * @throws RetinaException
     */
    public synchronized boolean add(byte[][] values, long timestamp) throws RetinaException
    {
        if (isFull())
        {
            return false;
        }
        int columnCount = schema.getChildren().size();
        checkArgument(values.length == columnCount,
                "Column values count does not match schema column count");

        for (int i = 0; i < values.length; ++i)
        {
            this.rowBatch.cols[i].add(new String(values[i]));
        }
        this.rowBatch.cols[columnCount].add(timestamp);
        this.rowBatch.size++;
        return true;
    }

    public VectorizedRowBatch getRowBatch()
    {
        return this.rowBatch;
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
