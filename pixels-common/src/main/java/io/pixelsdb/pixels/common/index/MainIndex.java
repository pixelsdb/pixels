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
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.index.IndexProto;

import java.io.Closeable;
import java.io.IOException;

/**
 * The main index of a table is the mapping from row id to the row offset in the data file.
 * Each version of each row has a unique row id.
 * The row id of each table is an unsigned int_64 increases from zero.
 *
 * @author hank
 * @create 2025-02-10
 */
public interface MainIndex extends Closeable
{
    /**
     * Get the tableId of this mainIndex.
     * @return the tableId
     */
    long getTableId();

    /**
     * Whether the main index implementation has a main index cache.
     * For main index with a cache, some methods (e.g., {@link #deleteRowIdRange(RowIdRange)})
     * may bypass the cache, thus {@link #flushCache()} should be called before these methods
     * to operate on the most fresh state of the main index.
     * @return true if cache exists
     */
    boolean hasCache();

    /**
     * Allocate rowId batch for single point index.
     * @param tableId the table id of single point index
     * @param numRowIds the rowId nums need to allocate
     * @return the RowIdBatch
     */
    IndexProto.RowIdBatch allocateRowIdBatch(long tableId, int numRowIds) throws RowIdException;

    /**
     * Get the physical location of a row given the row id
     * @param rowId the row id
     * @return the row location
     */
    IndexProto.RowLocation getLocation(long rowId) throws MainIndexException;

    /**
     * Put a single row id into the main index.
     * @param rowId the row id
     * @param rowLocation the location of the row id
     * @return true on success
     */
    boolean putEntry(long rowId, IndexProto.RowLocation rowLocation);

    /**
     * Delete range of row ids from the main index. This method only has effect on the persistent storage
     * of the main index. If there is a
     * {@link #getLocation(long)} of a row id within a deleted range should return null.
     * @param rowIdRange the row id range to be deleted
     * @return true on success
     */
    boolean deleteRowIdRange(RowIdRange rowIdRange) throws MainIndexException;

    /**
     * Flush the main index cache into persistent storage.
     * If cache does not exist (i.e., {@link #hasCache()} returns false), this method has no effect.
     * @param fileId the file id of which the cached index entries are to be flushed
     * @return true on success
     */
    boolean flushCache(long fileId) throws MainIndexException;

    /**
     * Flush the main index cache if exists and close the main index instance.
     * @throws IOException
     */
    @Override
    void close() throws IOException;
}
