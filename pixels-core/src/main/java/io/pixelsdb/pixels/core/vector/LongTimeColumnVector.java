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

import com.google.flatbuffers.FlatBufferBuilder;
import io.pixelsdb.pixels.core.flat.TimeColumnVectorFlat;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.PICOS_PER_MILLIS;

/**
 * TIME column vector in Trino-native picoseconds-of-day layout.
 * <p>
 * Stores values in a {@code long[]} so they can be zero-copied into Trino
 * {@code LongArrayBlock}. Pixels on-disk and serialized TIME is milliseconds
 * of day; conversion to picoseconds happens while reading.
 *
 * @author gengdy
 * @create 2026-08-17
 */
public class LongTimeColumnVector extends LongColumnVector
{
    private final int precision;

    public LongTimeColumnVector(int precision)
    {
        this(VectorizedRowBatch.DEFAULT_SIZE, precision);
    }

    public LongTimeColumnVector(int len, int precision)
    {
        super(len);
        if (precision != 3)
        {
            // TODO: support more precisions.
            throw new UnsupportedOperationException("Time type currently only supports precision 3");
        }
        this.precision = precision;
    }

    public int getPrecision()
    {
        return precision;
    }

    public void set(int elementNum, long picoOfDay)
    {
        if (elementNum >= writeIndex)
        {
            writeIndex = elementNum + 1;
        }
        this.isNull[elementNum] = false;
        this.vector[elementNum] = picoOfDay;
    }

    @Override
    public byte getFlatBufferType()
    {
        throw new UnsupportedOperationException("LongTimeColumnVector is a read-only vector layout");
    }

    @Override
    public int serialize(FlatBufferBuilder builder)
    {
        throw new UnsupportedOperationException("LongTimeColumnVector is a read-only vector layout");
    }

    /**
     * Deserialize native millis-of-day TIME directly into picoseconds-of-day layout.
     */
    public static LongTimeColumnVector deserialize(TimeColumnVectorFlat flat)
    {
        LongTimeColumnVector vector = new LongTimeColumnVector(flat.base().length(), flat.precision());
        for (int i = 0; i < flat.timesLength(); ++i)
        {
            vector.vector[i] = (long) flat.times(i) * PICOS_PER_MILLIS;
        }
        vector.deserializeBase(flat.base());
        vector.memoryUsage += (long) (Long.BYTES - Integer.BYTES) * vector.length;
        return vector;
    }
}
