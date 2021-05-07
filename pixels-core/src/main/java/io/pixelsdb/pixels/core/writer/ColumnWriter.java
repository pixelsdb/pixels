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

/**
 * pixels
 *
 * @author guodong
 */
public interface ColumnWriter
{
    /**
     * Create a column writer according to the data type.
     * @param schema the data type.
     * @param pixelStride
     * @param isEncoding set true if enable data encoding.
     * @return
     */
    public static ColumnWriter newColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        switch (schema.getCategory())
        {
            case BOOLEAN:
                return new BooleanColumnWriter(schema, pixelStride, isEncoding);
            case BYTE:
                return new ByteColumnWriter(schema, pixelStride, isEncoding);
            case SHORT:
            case INT:
            case LONG:
                return new IntegerColumnWriter(schema, pixelStride, isEncoding);
            case FLOAT:
                return new FloatColumnWriter(schema, pixelStride, isEncoding);
            case DOUBLE:
                return new DoubleColumnWriter(schema, pixelStride, isEncoding);
            case STRING:
                return new StringColumnWriter(schema, pixelStride, isEncoding);
            case CHAR:
                return new CharColumnWriter(schema, pixelStride, isEncoding, schema.getMaxLength());
            case VARCHAR:
                return new VarcharColumnWriter(schema, pixelStride, isEncoding, schema.getMaxLength());
            case BINARY:
                return new BinaryColumnWriter(schema, pixelStride, isEncoding);
            case DATE:
                return new DateColumnWriter(schema, pixelStride, isEncoding);
            case TIME:
                return new TimeColumnWriter(schema, pixelStride, isEncoding);
            case TIMESTAMP:
                return new TimestampColumnWriter(schema, pixelStride, isEncoding);
            default:
                throw new IllegalArgumentException("Bad schema type: " + schema.getCategory());
        }
    }

    int write(ColumnVector vector, int length)
            throws IOException;

    byte[] getColumnChunkContent();

    int getColumnChunkSize();

    PixelsProto.ColumnChunkIndex.Builder getColumnChunkIndex();

    PixelsProto.ColumnStatistic.Builder getColumnChunkStat();

    PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding();

    StatsRecorder getColumnChunkStatRecorder();

    void reset();

    void flush()
            throws IOException;

    void close()
            throws IOException;
}
