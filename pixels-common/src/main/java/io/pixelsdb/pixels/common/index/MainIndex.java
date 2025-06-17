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

import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.index.IndexProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * The main index of a table is the mapping from row id to data file.
 * Each version of each row has a unique row id.
 * The row id of each table is an unsigned int_64 increases from zero.
 *
 * @author hank
 * @create 2025-02-10
 */
public interface MainIndex extends Closeable
{
    /**
     * Get the physical location of a row given the row id
     * @param rowId the row id
     * @return the row location
     */
    IndexProto.RowLocation getLocation(long rowId);

    /**
     * Put a single row id into the main index, in order to enable simultaneous insert into the main index while
     * inserting a single secondary index.
     * @param rowId the row id
     * @param rowLocation the location of the row id
     * @return true on success
     */
    boolean putRowId(long rowId, IndexProto.RowLocation rowLocation);

    /**
     * Delete a single row id from the main index. {@link #getLocation(long)} of a row id within a deleted range
     * should return null.
     * @param rowId the row id range to be deleted
     * @return true on success
     */
    boolean deleteRowId(long rowId);

    /**
     * Put the row ids from a row group into the main index. In pixels, we allocate the row ids in batches (ranges)
     * and put the row id range of a row group into the main index at once.
     * @param rowIdRangeOfRg the row id range of the row group
     * @param rgLocation the location of the row group
     * @return true on success
     */
    boolean putRowIdsOfRg(RowIdRange rowIdRangeOfRg, RgLocation rgLocation);

    /**
     * Delete a range of row ids from the main index. {@link #getLocation(long)} of a row id within a deleted range
     * should return null.
     * @param rowIdRange the row id range to be deleted
     * @return true on success
     */
    boolean deleteRowIdRange(RowIdRange rowIdRange);

    /**
     * Distribute row id for the secondary index.
     * If there isn't any row id in cache, get a range of row ids at once and put them into cache.
     *
     * @param entry the rowLocation of secondary index
     */
    boolean getRowId(SecondaryIndex.Entry entry) throws RowIdException;

    /**
     * Distribute row ids for the secondary index.
     * If there isn't enough row ids in cache, get a range of row ids at once and put them into cache.
     *
     * @param entries the rowLocation of secondary index
     */
    boolean getRgOfRowIds(List<SecondaryIndex.Entry> entries) throws RowIdException;

    /**
     * Persist the main index into persistent storage.
     * @return true on success
     */
    boolean persist();

    /**
     * Persist the main index and close it.
     * @throws IOException
     */
    @Override
    void close() throws IOException;

    /**
     * The location of the row group, composed of the file id and the row group id inside the file.
     */
    class RgLocation
    {
        /**
         * The file id of the file in pixels metadata, starts from 1.
         */
        private final long fileId;
        /**
         * The row group id inside the file, starts from 0.
         */
        private final int rowGroupId;

        public RgLocation(long fileId, int rowGroupId)
        {
            this.fileId = fileId;
            this.rowGroupId = rowGroupId;
        }

        public long getFileId()
        {
            return fileId;
        }

        public int getRowGroupId()
        {
            return rowGroupId;
        }
    }
}
