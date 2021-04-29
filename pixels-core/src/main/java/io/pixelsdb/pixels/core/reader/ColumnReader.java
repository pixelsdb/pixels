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
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * pixels column reader.
 * Read from file, and decode column values
 *
 * @author guodong, hank
 */
public abstract class ColumnReader implements Closeable
{
    private final TypeDescription type;

    int elementIndex = 0;
    boolean hasNull = true;

    public static ColumnReader newColumnReader(TypeDescription type)
    {
        switch (type.getCategory())
        {
            case BINARY:
                return new BinaryColumnReader(type);
            case BOOLEAN:
                return new BooleanColumnReader(type);
            case BYTE:
                return new ByteColumnReader(type);
            case CHAR:
                return new CharColumnReader(type);
            case SHORT:
            case INT:
            case LONG:
                return new IntegerColumnReader(type);
            case DOUBLE:
                return new DoubleColumnReader(type);
            case FLOAT:
                return new FloatColumnReader(type);
            case STRING:
                return new StringColumnReader(type);
            case DATE:
                return new DateColumnReader(type);
            case TIME:
                return new TimeColumnReader(type);
            case TIMESTAMP:
                return new TimestampColumnReader(type);
            case VARCHAR:
                return new VarcharColumnReader(type);
            default:
                throw new IllegalArgumentException("Bad schema type: " + type.getCategory());
        }
    }

    /**
     * Read values from input buffer.
     * Values after specified offset are gonna be put into the specified vector.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param offset   starting reading offset of values
     * @param size     number of values to read
     * @param vector   vector to read into
     * @throws java.io.IOException
     */
    public abstract void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                              int offset, int size, int pixelStride, final int vectorIndex,
                              ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
            throws IOException;

    public ColumnReader(TypeDescription type)
    {
        this.type = type;
    }

    public TypeDescription getType()
    {
        return type;
    }

    /**
     * Closes this column reader and releases any resources associated
     * with it. If the column reader is already closed then invoking this
     * method has no effect.
     * <p>
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    abstract public void close() throws IOException;
}
