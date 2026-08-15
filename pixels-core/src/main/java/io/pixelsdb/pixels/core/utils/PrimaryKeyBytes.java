/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.core.utils;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * v0 primary-key encoding: bare concatenation of per-column canonical bytes
 * from {@link TypeDescription#convertSqlStringToByte} /
 * {@link TypeDescription#convertColumnVectorToByte}.
 * <p>
 * SQL NULL is illegal in a primary key; encoded parts must be non-null.
 * </p>
 */
public final class PrimaryKeyBytes
{
    private PrimaryKeyBytes()
    {
    }

    /**
     * Concatenate already-encoded column key parts (v0).
     *
     * @throws IllegalArgumentException if any part is {@code null}
     */
    public static byte[] concat(byte[]... parts)
    {
        if (parts == null || parts.length == 0)
        {
            throw new IllegalArgumentException("Primary key must contain at least one column.");
        }
        int totalLen = 0;
        for (int i = 0; i < parts.length; i++)
        {
            if (parts[i] == null)
            {
                throw new IllegalArgumentException(
                        "Primary key column at ordinal " + i + " cannot be NULL.");
            }
            totalLen += parts[i].length;
        }
        if (parts.length == 1)
        {
            return parts[0];
        }
        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        for (byte[] part : parts)
        {
            buf.put(part);
        }
        return buf.array();
    }

    /**
     * Encode PK columns from SQL/CSV tokens already ordered as the primary key.
     */
    public static byte[] fromSqlStrings(List<TypeDescription> pkTypes, String[] pkValuesInOrder)
    {
        if (pkTypes == null || pkValuesInOrder == null || pkTypes.size() != pkValuesInOrder.length)
        {
            throw new IllegalArgumentException("Primary key types and values size mismatch.");
        }
        byte[][] parts = new byte[pkTypes.size()][];
        for (int i = 0; i < pkTypes.size(); i++)
        {
            parts[i] = pkTypes.get(i).convertSqlStringToByte(pkValuesInOrder[i]);
        }
        return concat(parts);
    }

    /**
     * Encode PK columns from a row batch using schema ordinals into {@code cols}.
     */
    public static byte[] fromColumnVectors(List<TypeDescription> pkTypes, ColumnVector[] cols,
                                           int[] ordinals, int row)
    {
        if (pkTypes == null || cols == null || ordinals == null || pkTypes.size() != ordinals.length)
        {
            throw new IllegalArgumentException("Primary key types and ordinals size mismatch.");
        }
        byte[][] parts = new byte[pkTypes.size()][];
        for (int i = 0; i < pkTypes.size(); i++)
        {
            int ordinal = ordinals[i];
            if (ordinal < 0 || ordinal >= cols.length)
            {
                throw new IllegalArgumentException(
                        "Primary key column at ordinal " + i + " maps to invalid vector index " + ordinal);
            }
            parts[i] = pkTypes.get(i).convertColumnVectorToByte(cols[ordinal], row);
        }
        return concat(parts);
    }
}
