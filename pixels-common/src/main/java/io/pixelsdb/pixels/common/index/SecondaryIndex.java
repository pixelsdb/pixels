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

import io.pixelsdb.pixels.index.IndexProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * @author hank
 * @create 2025-02-07
 */
public interface SecondaryIndex extends Closeable
{
    /**
     * If we want to add more secondary index schemes here, modify this enum.
     */
    enum Scheme
    {
        rocksdb,  // secondary index stored in rocksdb
        rockset;  // secondary index stored in rockset (rocksdb-cloud)

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

//    boolean putEntry(IndexProto.IndexKey key, long rowId, boolean unique);

    boolean putEntry(Entry entry);

    boolean putEntries(List<Entry> entries);

    boolean deleteEntry(IndexProto.IndexKey key);

    boolean deleteEntries(List<IndexProto.IndexKey> keys);

    /**
     * Close the secondary index. This method is to be used by the secondary index factory to close the
     * managed secondary index instances when the process is shutting down.
     * Users do not need to close the managed secondary index instances by themselves.
     * @throws IOException
     */
    @Override
    void close() throws IOException;

    class Entry
    {
        private final IndexProto.IndexKey key;
        private long rowId;
        private final boolean unique;

        public Entry(IndexProto.IndexKey key, long rowId, boolean unique)
        {
            this.key = key;
            this.rowId = rowId;
            this.unique = unique;
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
