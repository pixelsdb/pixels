/*
 * Copyright 2023 PixelsDB.
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
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.TimestampColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestTimestampColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimestampColumnWriter columnWriter = new TimestampColumnWriter(
                TypeDescription.createTimestamp(6), writerOption);
        TimestampColumnVector timestampColumnVector = new TimestampColumnVector(numRows, 6);
        timestampColumnVector.add(100L);
        timestampColumnVector.add(103L);
        timestampColumnVector.add(106L);
        timestampColumnVector.add(34L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(54L);
        timestampColumnVector.add(55L);
        timestampColumnVector.add(67L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(34L);
        timestampColumnVector.add(555L);
        timestampColumnVector.add(565L);
        timestampColumnVector.add(234L);
        timestampColumnVector.add(675L);
        timestampColumnVector.add(235L);
        timestampColumnVector.add(32434L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(6L);
        timestampColumnVector.add(7L);
        timestampColumnVector.add(65656565L);
        timestampColumnVector.add(3434L);
        timestampColumnVector.add(54578L);
        columnWriter.write(timestampColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimestampColumnReader columnReader = new TimestampColumnReader(TypeDescription.createTimestamp(6));
        TimestampColumnVector timestampColumnVector1 = new TimestampColumnVector(numRows, 6);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, timestampColumnVector1, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert timestampColumnVector1.noNulls == timestampColumnVector.noNulls;
            assert timestampColumnVector1.isNull[i] == timestampColumnVector.isNull[i];
            if (timestampColumnVector.noNulls || !timestampColumnVector.isNull[i])
            {
                assert timestampColumnVector1.times[i] == timestampColumnVector.times[i];
            }
        }
    }

    @Test
    public void testWithoutNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        TimestampColumnWriter columnWriter = new TimestampColumnWriter(
                TypeDescription.createTimestamp(6), writerOption);
        TimestampColumnVector timestampColumnVector = new TimestampColumnVector(numRows, 6);
        timestampColumnVector.add(100L);
        timestampColumnVector.add(103L);
        timestampColumnVector.add(106L);
        timestampColumnVector.add(34L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(54L);
        timestampColumnVector.add(55L);
        timestampColumnVector.add(67L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(34L);
        timestampColumnVector.add(555L);
        timestampColumnVector.add(565L);
        timestampColumnVector.add(234L);
        timestampColumnVector.add(675L);
        timestampColumnVector.add(235L);
        timestampColumnVector.add(32434L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(6L);
        timestampColumnVector.add(7L);
        timestampColumnVector.add(65656565L);
        timestampColumnVector.add(3434L);
        timestampColumnVector.add(54578L);
        columnWriter.write(timestampColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimestampColumnReader columnReader = new TimestampColumnReader(TypeDescription.createTimestamp(6));
        TimestampColumnVector timestampColumnVector1 = new TimestampColumnVector(numRows, 6);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, timestampColumnVector1, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert timestampColumnVector1.noNulls == timestampColumnVector.noNulls;
            assert timestampColumnVector1.isNull[i] == timestampColumnVector.isNull[i];
            if (timestampColumnVector.noNulls || !timestampColumnVector.isNull[i])
            {
                assert timestampColumnVector1.times[i] == timestampColumnVector.times[i];
            }
        }
    }

    @Test
    public void testSelected() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimestampColumnWriter columnWriter = new TimestampColumnWriter(
                TypeDescription.createTimestamp(6), writerOption);
        TimestampColumnVector timestampColumnVector = new TimestampColumnVector(numRows, 6);
        timestampColumnVector.add(100L);
        timestampColumnVector.add(103L);
        timestampColumnVector.add(106L);
        timestampColumnVector.add(34L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(54L);
        timestampColumnVector.add(55L);
        timestampColumnVector.add(67L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(34L);
        timestampColumnVector.add(555L);
        timestampColumnVector.add(565L);
        timestampColumnVector.add(234L);
        timestampColumnVector.add(675L);
        timestampColumnVector.add(235L);
        timestampColumnVector.add(32434L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(6L);
        timestampColumnVector.add(7L);
        timestampColumnVector.add(65656565L);
        timestampColumnVector.add(3434L);
        timestampColumnVector.add(54578L);
        columnWriter.write(timestampColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimestampColumnReader columnReader = new TimestampColumnReader(TypeDescription.createTimestamp(6));
        TimestampColumnVector timestampColumnVector1 = new TimestampColumnVector(numRows, 6);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, timestampColumnVector1, chunkIndex, selected);
        columnReader.close();

        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assert timestampColumnVector1.noNulls == timestampColumnVector.noNulls;
                assert timestampColumnVector1.isNull[j] == timestampColumnVector.isNull[i];
                if (timestampColumnVector.noNulls || !timestampColumnVector.isNull[i])
                {
                    assert timestampColumnVector1.times[j] == timestampColumnVector.times[i];
                }
                j++;
            }
        }
    }

    @Test
    public void testLarge() throws IOException
    {
        int numBatches = 15;
        int numRows = 1024;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10000).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimestampColumnWriter columnWriter = new TimestampColumnWriter(
                TypeDescription.createTimestamp(6), writerOption);

        TimestampColumnVector originVector = new TimestampColumnVector(numRows, 6);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            } else
            {
                originVector.add(1000L);
            }
        }

        for (int i = 0; i < numBatches; i++)
        {
            columnWriter.write(originVector, numRows);
        }
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimestampColumnReader columnReader = new TimestampColumnReader(TypeDescription.createTimestamp(6));
        TimestampColumnVector targetVector = new TimestampColumnVector(numBatches*numRows, 6);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numBatches*numRows,
                10000, 0, targetVector, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numBatches*numRows; i++)
        {
            assert targetVector.isNull[i] == originVector.isNull[i%numRows];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                assert targetVector.times[i] == originVector.times[i % numRows];
            }
        }
    }

    @Test
    public void testLargeFragmented() throws IOException
    {
        int numBatches = 15;
        int numRows = 1024;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10000).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimestampColumnWriter columnWriter = new TimestampColumnWriter(
                TypeDescription.createTimestamp(6), writerOption);

        TimestampColumnVector originVector = new TimestampColumnVector(numRows, 6);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            } else
            {
                originVector.add(1000L);
            }
        }

        for (int i = 0; i < numBatches; i++)
        {
            columnWriter.write(originVector, numRows);
        }
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimestampColumnReader columnReader = new TimestampColumnReader(TypeDescription.createTimestamp(6));
        TimestampColumnVector targetVector = new TimestampColumnVector(numBatches*numRows, 6);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        columnReader.read(buffer, encoding, 0, 123,
                10000, 0, targetVector, chunkIndex);
        columnReader.read(buffer, encoding, 123, 456,
                10000, 123, targetVector, chunkIndex);
        columnReader.read(buffer, encoding, 123+456, numBatches*numRows-123-456,
                10000, 123+456, targetVector, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numBatches*numRows; i++)
        {
            assert targetVector.isNull[i] == originVector.isNull[i%numRows];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                assert targetVector.times[i] == originVector.times[i % numRows];
            }
        }
    }
}
