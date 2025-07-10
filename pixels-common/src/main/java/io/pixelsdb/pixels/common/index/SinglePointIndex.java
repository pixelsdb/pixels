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
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.index.IndexProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
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

    long getUniqueRowId(IndexProto.IndexKey key);

    long[] getRowIds(IndexProto.IndexKey key);

    boolean putPrimaryEntry(Entry entry) throws MainIndexException, SinglePointIndexException;

    boolean putPrimaryEntries(List<Entry> entries) throws MainIndexException, SinglePointIndexException;

    boolean putSecondaryEntry(Entry entry) throws SinglePointIndexException;

    boolean putSecondaryEntries(List<Entry> entries) throws SinglePointIndexException;

    /**
     * Delete the primary index entry of the index key
     * @param indexKey the index key
     * @return the row location of the deleted index entry
     * @throws MainIndexException
     * @throws SinglePointIndexException
     */
    IndexProto.RowLocation deletePrimaryEntry(IndexProto.IndexKey indexKey) throws MainIndexException, SinglePointIndexException;

    /**
     * Delete the primary index entries of the index keys
     * @param indexKeys the index keys
     * @return the row locations of the deleted index entries
     * @throws MainIndexException
     * @throws SinglePointIndexException
     */
    List<IndexProto.RowLocation> deletePrimaryEntries(List<IndexProto.IndexKey> indexKeys) throws MainIndexException, SinglePointIndexException;

    /**
     * Delete the secondary index entry of the index key
     * @param indexKey the index key
     * @return the row id of the deleted index entry
     * @throws MainIndexException
     * @throws SinglePointIndexException
     */
    long deleteSecondaryEntry(IndexProto.IndexKey indexKey) throws SinglePointIndexException;

    /**
     * Delete the secondary index entries of the index keys
     * @param indexKeys the index keys
     * @return the row ids of the deleted index entries
     * @throws MainIndexException
     * @throws SinglePointIndexException
     */
    List<Long> deleteSecondaryEntries(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException;

    /**
     * Close the single point index. This method is to be used by the single point index factory to close the
     * managed single point index instances when the process is shutting down.
     * Users do not need to close the managed single point index instances by themselves.
     * @throws IOException
     */
    @Override
    void close() throws IOException;

    class Entry
    {
        private final IndexProto.IndexKey key;
        private long rowId;
        private final boolean unique;
        private final IndexProto.RowLocation rowLocation;

        public Entry(IndexProto.IndexKey key, long rowId, boolean unique, IndexProto.RowLocation rowLocation)
        {
            this.key = key;
            this.rowId = rowId;
            this.unique = unique;
            this.rowLocation = rowLocation;
        }

        public IndexProto.IndexKey getKey()
        {
            return key;
        }

        public long getRowId()
        {
            return rowId;
        }

        public boolean getIsUnique() { return unique; }

        public IndexProto.RowLocation getRowLocation()
        {
            return rowLocation;
        }

        public void setRowId(long rowId) {
            this.rowId = rowId;
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Entry))
            {
                return false;
            }
            Entry that = (Entry) other;
            if (this.key == null || that.key == null)
            {
                return this.key == that.key && this.rowId == that.rowId;
            }
            return this.key.equals(that.key) && this.rowId == that.rowId;
        }
    }
}
