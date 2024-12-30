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
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import static io.pixelsdb.pixels.core.TypeDescription.MAX_SHORT_DECIMAL_PRECISION;
import static java.util.Objects.requireNonNull;

/**
 * pixels column reader.
 * Read from file, and decode column values
 *
 * @author guodong, hank
 * @update 2024-01-08 add isNullSkipBits
 *
 */
public abstract class ColumnReader implements Closeable
{
    final TypeDescription type;

    int elementIndex = 0;
    boolean hasNull = true;
    /**
     * The current offset in the input buffer of this column chunk from which to decompact the isNull array.
     * It starts from the offset of the bit-packed isNull array in this column chunk.
     */
    int isNullOffset = 0;
    /**
     * The number of bits to skip in the first byte (start from isNullOffset) when decompacting the isNull array.
     */
    int isNullSkipBits = 0;

    public static ColumnReader newColumnReader(TypeDescription type, PixelsReaderOption option)
    {
        switch (type.getCategory())
        {
            case BOOLEAN:
                return new BooleanColumnReader(type);
            case BYTE:
                return new ByteColumnReader(type);
            case SHORT:
            case INT:
                if (option.isReadIntColumnAsIntVector())
                {
                    return new IntColumnReader(type);
                }
                else
                {
                    return new LongColumnReader(type);
                }
            case LONG:
                return new LongColumnReader(type);
            case DOUBLE:
                return new DoubleColumnReader(type);
            case DECIMAL: // Issue #196: precision and scale are passed through type.
                if (type.getPrecision() <= MAX_SHORT_DECIMAL_PRECISION)
                    return new DecimalColumnReader(type);
                else
                    return new LongDecimalColumnReader(type);
            case FLOAT:
                return new FloatColumnReader(type);
            case CHAR:
                return new CharColumnReader(type);
            case VARCHAR:
                return new VarcharColumnReader(type);
            case STRING:
                return new StringColumnReader(type);
            case DATE:
                return new DateColumnReader(type);
            case TIME:
                return new TimeColumnReader(type);
            case TIMESTAMP:
                return new TimestampColumnReader(type);
            case BINARY:
                return new BinaryColumnReader(type);
            case VARBINARY:
                return new VarbinaryColumnReader(type);
            case VECTOR:
                return new VectorColumnReader(type);
            default:
                throw new IllegalArgumentException("bad column type: " + type.getCategory());
        }
    }

    public ColumnReader(TypeDescription type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    /**
     * Read values from input buffer.
     * Values after specified offset are going to be put into the specified vector.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param offset   starting reading offset of values
     * @param size     number of values to read
     * @param pixelStride the stride (number of rows) in a pixels.
     * @param vectorIndex the index from where we start reading values into the vector
     * @param vector   vector to read values into
     * @param chunkIndex the metadata of the column chunk to read.
     * @throws java.io.IOException
     */
    public abstract void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                              int offset, int size, int pixelStride, final int vectorIndex,
                              ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex) throws IOException;

    /**
     * Read selected values from input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param offset   starting reading offset of values
     * @param size     number of values to read
     * @param pixelStride the stride (number of rows) in a pixels.
     * @param vectorIndex the index from where we start reading values into the vector
     * @param vector   vector to read values into
     * @param chunkIndex the metadata of the column chunk to read.
     * @param selected whether the value is selected, use the vectorIndex as the 0 offset of the selected
     * @throws IOException
     */
    public abstract void readSelected(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                                      int offset, int size, int pixelStride, final int vectorIndex,
                                      ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex, Bitmap selected) throws IOException;

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
    public abstract void close() throws IOException;
}
