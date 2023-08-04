/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.IOException;
import java.nio.ByteOrder;

import static io.pixelsdb.pixels.core.TypeDescription.SHORT_DECIMAL_MAX_PRECISION;

/**
 * pixels
 *
 * @author guodong
 */
public interface ColumnWriter
{
    /**
     * Create a column writer according to the data type.
     * @param type the data type.
     * @param pixelStride
     * @param isEncoding set true if enable data encoding.
     * @return
     */
    static ColumnWriter newColumnWriter(TypeDescription type, int pixelStride,
                                        boolean isEncoding, ByteOrder byteOrder)
    {
        switch (type.getCategory())
        {
            case BOOLEAN:
                return new BooleanColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case BYTE:
                return new ByteColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case SHORT:
            case INT:
            case LONG:
                return new IntegerColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case FLOAT:
                return new FloatColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case DOUBLE:
                return new DoubleColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case DECIMAL: // Issue #196: precision and scale are passed through type.
                if (type.getPrecision() <= SHORT_DECIMAL_MAX_PRECISION)
                    return new DecimalColumnWriter(type, pixelStride, isEncoding, byteOrder);
                else
                    return new LongDecimalColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case STRING:
                return new StringColumnWriter(type, pixelStride, isEncoding, byteOrder);
            // Issue #196: max length of char, varchar, binary, and varbinary, are passed through type.
            case CHAR:
                return new CharColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case VARCHAR:
                return new VarcharColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case BINARY:
                return new BinaryColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case VARBINARY:
                return new VarbinaryColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case DATE:
                return new DateColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case TIME:
                return new TimeColumnWriter(type, pixelStride, isEncoding, byteOrder);
            case TIMESTAMP:
                return new TimestampColumnWriter(type, pixelStride, isEncoding, byteOrder);
            default:
                throw new IllegalArgumentException("Bad schema type: " + type.getCategory());
        }
    }

    int write(ColumnVector vector, int length) throws IOException;

    byte[] getColumnChunkContent();

    int getColumnChunkSize();

    PixelsProto.ColumnChunkIndex.Builder getColumnChunkIndex();

    PixelsProto.ColumnStatistic.Builder getColumnChunkStat();

    PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding();

    StatsRecorder getColumnChunkStatRecorder();

    void reset();

    void flush() throws IOException;

    void close() throws IOException;
}
