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
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.index.IndexProto;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Batch request row ids and return available row id
 */
public class RowIdAllocator
{
    private final long tableId;
    private final int batchSize;
    private final IndexService indexService;

    // --- current batch status ---
    private long currentBatchStart = -1;
    private int allocatedCountInBatch = 0;
    private int currentBatchLength = 0;

    private final ReentrantLock lock = new ReentrantLock();

    public RowIdAllocator(long tableId, int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new IllegalArgumentException("batchSize must be positive.");
        }
        this.tableId = tableId;
        this.batchSize = batchSize;
        this.indexService = IndexService.Instance();
    }

    /**
     * get a unique rowId
     * @return
     * @throws RetinaException
     */
    public long getRowId() throws RetinaException
    {
        this.lock.lock();
        try
        {
            if (this.currentBatchStart == -1 || this.allocatedCountInBatch >= this.currentBatchLength)
            {
                fetchNewBatch();
            }
            long nextRowId = this.currentBatchStart + this.allocatedCountInBatch;
            this.allocatedCountInBatch++;
            return nextRowId;
        } finally
        {
            this.lock.unlock();
        }
    }

    private void fetchNewBatch() throws RetinaException
    {
        IndexProto.RowIdBatch newBatch = this.indexService.allocateRowIdBatch(
                this.tableId, this.batchSize);
        if (newBatch == null || newBatch.getLength() <= 0)
        {
            throw new RetinaException("failed to get row id batch");
        }
        this.currentBatchStart = newBatch.getRowIdStart();
        this.currentBatchLength = newBatch.getLength();
        this.allocatedCountInBatch = 0;
    }
}
