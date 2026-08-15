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
package io.pixelsdb.pixels.core.vector;

/**
 * Boolean column vector.
 * <p>
 * Reuses {@link ByteColumnVector} storage (0/1 in {@code byte[]}), but interprets
 * string literals with boolean semantics rather than {@link Byte#parseByte(String)}.
 * </p>
 */
public class BooleanColumnVector extends ByteColumnVector
{
    public BooleanColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    public BooleanColumnVector(int len)
    {
        super(len);
    }

    @Override
    public void add(String value)
    {
        String normalized = value.trim();
        if (normalized.equals("1") || normalized.equalsIgnoreCase("true"))
        {
            add(true);
            return;
        }
        if (normalized.equals("0") || normalized.equalsIgnoreCase("false"))
        {
            add(false);
            return;
        }
        throw new IllegalArgumentException("Invalid BOOLEAN value: " + value);
    }
}
