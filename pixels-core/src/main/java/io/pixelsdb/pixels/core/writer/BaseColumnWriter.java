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
import io.pixelsdb.pixels.core.encoding.Encoder;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;

import static java.util.Objects.requireNonNull;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class BaseColumnWriter implements ColumnWriter
{
    final int pixelStride;                     // indicate num of elements in a pixel
    final boolean isEncoding;                  // indicate if encoding enabled during writing
    final ByteOrder byteOrder;                 // indicate the endianness used during writing
    final boolean[] isNull;
    private final PixelsProto.ColumnChunkIndex.Builder columnChunkIndex;
    private final PixelsProto.ColumnStatistic.Builder columnChunkStat;

    final StatsRecorder pixelStatRecorder;
    final TypeDescription type;
    private final StatsRecorder columnChunkStatRecorder;

    private int lastPixelPosition = 0;                 // ending offset of last pixel in the column chunk
    private int curPixelPosition = 0;                  // current offset of this pixel in the column chunk. this is a relative value inside each column chunk.

    int curPixelEleIndex = 0;                  // index of elements in previous vector
    int curPixelVectorIndex = 0;               // index of the element to write in the current vector
    int curPixelIsNullIndex = 0;               // index of isNull in previous vector

    Encoder encoder;
    boolean hasNull = false;

    final ByteArrayOutputStream outputStream;  // column chunk content
    private final ByteArrayOutputStream isNullStream;  // column chunk isNull

    public BaseColumnWriter(TypeDescription type, int pixelStride, boolean isEncoding, ByteOrder byteOrder)
    {
        this.type = requireNonNull(type, "type is null");
        this.pixelStride = pixelStride;
        this.isEncoding = isEncoding;
        this.byteOrder = requireNonNull(byteOrder, "byteOrder is null");
        this.isNull = new boolean[pixelStride];

        this.columnChunkIndex = PixelsProto.ColumnChunkIndex.newBuilder()
                .setLittleEndian(byteOrder.equals(ByteOrder.LITTLE_ENDIAN));
        this.columnChunkStat = PixelsProto.ColumnStatistic.newBuilder();
        this.pixelStatRecorder = StatsRecorder.create(type);
        this.columnChunkStatRecorder = StatsRecorder.create(type);

        // todo a good estimation of chunk size is needed as the initial size of output stream
        this.outputStream = new ByteArrayOutputStream(pixelStride);
        this.isNullStream = new ByteArrayOutputStream(pixelStride);
    }

    /**
     * Write ColumnVector
     * <p>
     * Serialize vector into {@code ByteBufferOutputStream}.
     * Update pixel statistics and positions.
     * Update column chunk statistics.
     *
     * @param vector vector
     * @param size   size of vector
     * @return size in bytes of the current column chunk
     */
    @Override
    public abstract int write(ColumnVector vector, int size) throws IOException;

    /**
     * Get byte array of column chunk content
     */
    @Override
    public byte[] getColumnChunkContent()
    {
        return outputStream.toByteArray();
    }

    /**
     * Get column chunk size in bytes
     */
    public int getColumnChunkSize()
    {
        return outputStream.size();
    }

    public PixelsProto.ColumnChunkIndex.Builder getColumnChunkIndex()
    {
        return columnChunkIndex;
    }

    public PixelsProto.ColumnStatistic.Builder getColumnChunkStat()
    {
        return columnChunkStatRecorder.serialize();
    }

    public StatsRecorder getColumnChunkStatRecorder()
    {
        return columnChunkStatRecorder;
    }

    public PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding()
    {
        return PixelsProto.ColumnEncoding.newBuilder()
                .setKind(PixelsProto.ColumnEncoding.Kind.NONE);
    }

    @Override
    public void flush() throws IOException
    {
        if (curPixelEleIndex > 0)
        {
            newPixel();
        }
        // record isNull offset in the column chunk
        columnChunkIndex.setIsNullOffset(outputStream.size());
        // flush out isNullStream
        isNullStream.writeTo(outputStream);
    }

    void newPixel() throws IOException
    {
        // isNull
        if (hasNull)
        {
            isNullStream.write(BitUtils.bitWiseCompact(isNull, curPixelIsNullIndex));
            pixelStatRecorder.setHasNull();
        }
        // update position of current pixel
        curPixelPosition = outputStream.size();
        // set current pixel element count to 0 for the next batch pixel writing
        curPixelEleIndex = 0;
        curPixelVectorIndex = 0;
        curPixelIsNullIndex = 0;
        // update column chunk stat
        columnChunkStatRecorder.merge(pixelStatRecorder);
        // add current pixel stat and position info to columnChunkIndex
        PixelsProto.PixelStatistic.Builder pixelStat =
                PixelsProto.PixelStatistic.newBuilder();
        pixelStat.setStatistic(pixelStatRecorder.serialize());
        columnChunkIndex.addPixelPositions(lastPixelPosition);
        columnChunkIndex.addPixelStatistics(pixelStat.build());
        // update lastPixelPosition to current one
        lastPixelPosition = curPixelPosition;
        // reset current pixel stat recorder
        pixelStatRecorder.reset();
        // reset hasNull
        hasNull = false;
    }

    @Override
    public void reset()
    {
        lastPixelPosition = 0;
        curPixelPosition = 0;
        columnChunkIndex.clear();
        columnChunkStat.clear();
        pixelStatRecorder.reset();
        columnChunkStatRecorder.reset();
        outputStream.reset();
        isNullStream.reset();
    }

    @Override
    public void close()
            throws IOException
    {
        outputStream.close();
        isNullStream.close();
    }
}
