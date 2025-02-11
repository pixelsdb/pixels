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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * @author hank
 * @create 2025-02-07
 */
public interface SecondaryIndex
{
    /**
     * If we want to add more secondary index schemes here, modify this enum.
     */
    enum Scheme
    {
        rocksdb;  // secondary index stored in rocksdb

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

    long getUniqueRowId(ByteBuffer key);

    long[] getRowIds(ByteBuffer key);

    boolean putEntry(ByteBuffer key, long rowId);

    boolean putEntries(Map<ByteBuffer, Long> entries);

    boolean deleteEntry(ByteBuffer key);

    boolean deleteEntries(List<ByteBuffer> keys);

    /**
     * Close the secondary index. This method is to be used by the secondary index factory to close the
     * managed secondary index instances when the process is shutting down.
     * Users do not need to close the managed secondary index instances by themselves.
     * @throws IOException
     */
    void close() throws IOException;
}
