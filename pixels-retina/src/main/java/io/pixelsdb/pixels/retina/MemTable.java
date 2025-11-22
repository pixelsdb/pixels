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

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

/**
 * MemTable is an in-memory table that stores data before it is flushed to disk.
 */
public class MemTable implements Referenceable
{
    private final ReferenceCounter refCounter = new ReferenceCounter();
    private final long id;  // unique identifier
    private final TypeDescription schema;
    private final VectorizedRowBatch rowBatch;


    private final long fileId;
    private final int startIndex;
    private final int length;

    public MemTable(long id, TypeDescription schema, int size, int mode,
                    long fileId, int startIndex, int length)
    {
        this.id = id;
        this.schema = schema;
        this.rowBatch = schema.createRowBatchWithHiddenColumn(size, mode);
        this.fileId = fileId;
        this.startIndex = startIndex;
        this.length = length;

        this.refCounter.ref(); // init reference count
    }

    /**
     * Adds a record with all column values and a timestamp.
     *
     * @param values
     * @param timestamp
     * @throws RetinaException
     */
    public synchronized int add(byte[][] values, long timestamp) throws RetinaException
    {
        if (isFull())
        {
            return -1;
        }
        for (int i = 0; i < values.length; ++i)
        {
            this.rowBatch.cols[i].add(values[i]);
        }
        this.rowBatch.cols[schema.getChildren().size()].add(timestamp);
        return this.rowBatch.size++;
    }

    public long getId()
    {
        return this.id;
    }

    public long getFileId()
    {
        return this.fileId;
    }

    public int getStartIndex()
    {
        return this.startIndex;
    }

    public int getLength()
    {
        return this.length;
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
