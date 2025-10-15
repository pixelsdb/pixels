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
import java.util.List;

/**
 * The main index of a table is the mapping from row id to the row offset in the data file.
 * Each version of each row has a unique row id.
 * The row id of each table is an unsigned int_64 increases monotonically starting from zero.
 *
 * @author hank
 * @create 2025-02-10
 */
public interface MainIndex extends Closeable
{
    /**
     * If we want to add more single point index schemes here, modify this enum.
     */
    enum Scheme
    {
        sqlite;  // main index stored in sqlite

        /**
         * Case-insensitive parsing from String name to enum value.
         * @param value the name of main index scheme.
         * @return the main index scheme if the value is valid
         */
        public static Scheme from(String value)
        {
            return valueOf(value.toLowerCase());
        }

        /**
         * @param value name of the main index scheme
         * @return true of the value is a valid main index scheme
         */
        public static boolean isValid(String value)
        {
            for (Scheme scheme : values())
            {
                if (scheme.equals(value))
                {
                    return true;
                }
            }
            return false;
        }

        public boolean equals(String other)
        {
            return this.toString().equalsIgnoreCase(other);
        }

        public boolean equals(Scheme other)
        {
            // enums in Java can be compared using '=='.
            return this == other;
        }
    }

    /**
     * @return the tableId of this mainIndex
     */
    long getTableId();

    /**
     * Whether the main index implementation has a main index cache.
     * For main index with a cache, some methods (e.g., {@link #deleteRowIdRange(RowIdRange)})
     * may bypass the cache, thus {@link #flushCache(long fileId)} should be called before these methods
     * to operate on the most fresh state of the main index.
     * @return true if cache exists
     */
    boolean hasCache();

    /**
     * Allocate rowId batch for single point index.
     * <br/><b>For better performance, use consistent numRowIds when calling this method.</b>
     * @param tableId the table id of single point index
     * @param numRowIds the number of row ids to allocate
     * @return the RowIdBatch
     */
    IndexProto.RowIdBatch allocateRowIdBatch(long tableId, int numRowIds) throws RowIdException;

    /**
     * Get the physical location of a row given the row id.
     * @param rowId the row id
     * @return the row location, or null if not found
     */
    IndexProto.RowLocation getLocation(long rowId) throws MainIndexException;

    /**
     * Get the physical locations of a batch of rows given the row ids.
     * @param rowIds the row ids
     * @return the row locations, empty if not found
     */
    List<IndexProto.RowLocation> getLocations(List<Long> rowIds) throws MainIndexException;

    /**
     * Put a single row id into the main index.
     * @param rowId the row id
     * @param rowLocation the location of the row id
     * @return true on success
     */
    boolean putEntry(long rowId, IndexProto.RowLocation rowLocation);

    /**
     * Put a batch of row ids into the main index given the primary index entries.
     * @param primaryEntries the primary index entries that contain the row ids and row locations
     * @return true on success for each entry
     */
    List<Boolean> putEntries(List<IndexProto.PrimaryIndexEntry> primaryEntries);

    /**
     * Delete a range of row ids from the main index. This method only has effect on the persistent storage
     * of the main index.
     * {@link #getLocation(long)} of a row id within a deleted range returns null.
     * @param rowIdRange the row id range to be deleted
     * @return true on success
     */
    boolean deleteRowIdRange(RowIdRange rowIdRange) throws MainIndexException;

    /**
     * Flush the main index cache into persistent storage.
     * If cache does not exist (i.e., {@link #hasCache()} returns false), this method has no effect.
     * <p/>
     * <b>Note: row ids of a flushed file id should not be put into the main index again.</b>
     * @param fileId the file id of which the cached index entries are to be flushed
     * @return true on success
     */
    boolean flushCache(long fileId) throws MainIndexException;

    /**
     * Flush the main index cache if exists and close the main index instance.
     * This method is to be used by the main index factory to close the
     * managed main index instances when the process is shutting down.
     * <p/>
     * <b>Note: Users do not need to close the managed main index instances by themselves.</b>
     */
    @Override
    @Deprecated
    void close() throws IOException;

    /**
     * Close the index and remove it from the storage. This method is idempotent.
     * @return true if success
     */
    boolean closeAndRemove() throws MainIndexException;
}
