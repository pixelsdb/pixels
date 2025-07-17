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

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.index.IndexProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * This class is used to create an instance for each single point index. It maintains the mapping from
 * the index key (the value of the key column(s)) to the unique row ids in the table.
 * @author hank
 * @create 2025-02-07
 * @update 2025-06-22 hank: rename from SecondaryIndex to SinglePointIndex
 */
public interface SinglePointIndex extends Closeable
{
    /**
     * If we want to add more single point index schemes here, modify this enum.
     */
    enum Scheme
    {
        rocksdb,  // single point index stored in rocksdb
        rockset;  // single point index stored in rockset (rocksdb-cloud)

        /**
         * Case-insensitive parsing from String name to enum value.
         * @param value the name of storage scheme.
         * @return
         */
        public static Scheme from(String value)
        {
            return valueOf(value.toLowerCase());
        }

        /**
         * Whether the value is a valid storage scheme.
         * @param value
         * @return
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
     * @return the table id of this single point index.
     */
    long getTableId();

    /**
     * @return the index id of this single point index.
     */
    long getIndexId();
    /**
     * Get the row id of an index key. This should be only used on a unique single point index.
     * @param key the index key
     * @return the row id
     * @throws SinglePointIndexException
     */
    long getUniqueRowId(IndexProto.IndexKey key) throws SinglePointIndexException;

    /**
     * Get the row ids of an index key. This should be only used on a non-unique single point index.
     * @param key the index key
     * @return the row ids
     * @throws SinglePointIndexException
     */
    List<Long> getNonUniqueRowIds(IndexProto.IndexKey key) throws SinglePointIndexException;

    /**
     * Put an entry into this single point index.
     * @param key the index key
     * @param rowId the row id in the table
     * @param unique whether this index is unique
     * @return true if the index entry is put successfully
     * @throws SinglePointIndexException
     */
    boolean putEntry(IndexProto.IndexKey key, long rowId, boolean unique)
            throws SinglePointIndexException;

    /**
     * Put the index entries of a primary index. Only the index key ({@link io.pixelsdb.pixels.index.IndexProto.IndexKey})
     * and the row ids are put into this single point index.
     * @param entries the primary index entries
     * @return true if the index entries are put successfully
     * @throws SinglePointIndexException
     */
    boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries)
            throws SinglePointIndexException;

    /**
     * Put the index entries of a secondary index. Only the index key ({@link io.pixelsdb.pixels.index.IndexProto.IndexKey})
     * and the row ids are put into this single point index.
     * @param entries the secondary index entries
     * @return true if the index entries are put successfully
     * @throws SinglePointIndexException
     */
    boolean putSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries)
            throws SinglePointIndexException;

    /**
     * Delete the index entry of the index key
     * @param indexKey the index key
     * @return the row id of the deleted index entry
     * @throws SinglePointIndexException
     */
    long deleteEntry(IndexProto.IndexKey indexKey)
            throws SinglePointIndexException;

    /**
     * Delete the index entries of the index keys
     * @param indexKeys the index keys
     * @return the row ids of the deleted index entries
     * @throws SinglePointIndexException
     */
    List<Long> deleteEntries(List<IndexProto.IndexKey> indexKeys)
            throws SinglePointIndexException;

    /**
     * Close the single point index. This method is to be used by the single point index factory to close the
     * managed single point index instances when the process is shutting down.
     * Users do not need to close the managed single point index instances by themselves.
     * @throws IOException
     */
    @Override
    @Deprecated
    void close() throws IOException;
}
