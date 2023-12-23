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

import static io.pixelsdb.pixels.core.TypeDescription.MAX_SHORT_DECIMAL_PRECISION;

/**
 * Interface for Pixels column writer.
 *
 * @author guodong, hank
 * @update 2023-08-16 Chamonix: support nulls padding
 */
public interface ColumnWriter
{
    /**
     * Create a column writer according to the data type.
     * @param type the data type
     * @param writerOption the writer option applied on the column
     * @return
     */
    static ColumnWriter newColumnWriter(TypeDescription type, PixelsWriterOption writerOption)
    {
        switch (type.getCategory())
        {
            case BOOLEAN:
                return new BooleanColumnWriter(type, writerOption);
            case BYTE:
                return new ByteColumnWriter(type, writerOption);
            case SHORT:
            case INT:
            case LONG:
                return new IntegerColumnWriter(type, writerOption);
            case FLOAT:
                return new FloatColumnWriter(type, writerOption);
            case DOUBLE:
                return new DoubleColumnWriter(type, writerOption);
            case DECIMAL: // Issue #196: precision and scale are passed through type.
                if (type.getPrecision() <= MAX_SHORT_DECIMAL_PRECISION)
                    return new DecimalColumnWriter(type, writerOption);
                else
                    return new LongDecimalColumnWriter(type, writerOption);
            case STRING:
                return new StringColumnWriter(type, writerOption);
            // Issue #196: max length of char, varchar, binary, and varbinary, are passed through type.
            case CHAR:
                return new CharColumnWriter(type, writerOption);
            case VARCHAR:
                return new VarcharColumnWriter(type, writerOption);
            case BINARY:
                return new BinaryColumnWriter(type, writerOption);
            case VARBINARY:
                return new VarbinaryColumnWriter(type, writerOption);
            case DATE:
                return new DateColumnWriter(type, writerOption);
            case TIME:
                return new TimeColumnWriter(type, writerOption);
            case TIMESTAMP:
                return new TimestampColumnWriter(type, writerOption);
            case VECTOR:
                return new VectorColumnWriter(type, writerOption);
            default:
                throw new IllegalArgumentException("Bad schema type: " + type.getCategory());
        }
    }

    int write(ColumnVector vector, int length) throws IOException;

    byte[] getColumnChunkContent();

    int getColumnChunkSize();

    boolean decideNullsPadding(PixelsWriterOption writerOption);

    PixelsProto.ColumnChunkIndex.Builder getColumnChunkIndex();

    PixelsProto.ColumnStatistic.Builder getColumnChunkStat();

    PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding();

    StatsRecorder getColumnChunkStatRecorder();

    void reset();

    void flush() throws IOException;

    void close() throws IOException;
}
