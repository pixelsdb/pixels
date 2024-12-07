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
import io.pixelsdb.pixels.core.vector.FloatColumnVector;
import io.pixelsdb.pixels.core.writer.FloatColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestFloatColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        FloatColumnWriter columnWriter = new FloatColumnWriter(
                TypeDescription.createFloat(), writerOption);
        FloatColumnVector floatColumnVector = new FloatColumnVector(numRows);
        floatColumnVector.add(100.22F);
        floatColumnVector.add(103.32F);
        floatColumnVector.add(106.43F);
        floatColumnVector.add(34.10F);
        floatColumnVector.addNull();
        floatColumnVector.add(54.09F);
        floatColumnVector.add(55.00F);
        floatColumnVector.add(67.23F);
        floatColumnVector.addNull();
        floatColumnVector.add(34.58F);
        floatColumnVector.add(555.98F);
        floatColumnVector.add(565.76F);
        floatColumnVector.add(234.11F);
        floatColumnVector.add(675.34F);
        floatColumnVector.add(235.58F);
        floatColumnVector.add(32434.68F);
        floatColumnVector.addNull();
        floatColumnVector.add(6.66F);
        floatColumnVector.add(7.77F);
        floatColumnVector.add(65656565.20F);
        floatColumnVector.add(3434.11F);
        floatColumnVector.add(54578.22F);
        columnWriter.write(floatColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        FloatColumnReader columnReader = new FloatColumnReader(TypeDescription.createFloat());
        FloatColumnVector floatColumnVector1 = new FloatColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, floatColumnVector1, chunkIndex);
        for (int i = 0; i < numRows; ++i)
        {
            assert floatColumnVector1.noNulls == floatColumnVector.noNulls;
            assert floatColumnVector1.isNull[i] == floatColumnVector.isNull[i];
            if (floatColumnVector.noNulls || !floatColumnVector.isNull[i])
            {
                assert floatColumnVector1.vector[i] == floatColumnVector.vector[i];
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
        FloatColumnWriter columnWriter = new FloatColumnWriter(
                TypeDescription.createFloat(), writerOption);
        FloatColumnVector floatColumnVector = new FloatColumnVector(numRows);
        floatColumnVector.add(100.22F);
        floatColumnVector.add(103.32F);
        floatColumnVector.add(106.43F);
        floatColumnVector.add(34.10F);
        floatColumnVector.addNull();
        floatColumnVector.add(54.09F);
        floatColumnVector.add(55.00F);
        floatColumnVector.add(67.23F);
        floatColumnVector.addNull();
        floatColumnVector.add(34.58F);
        floatColumnVector.add(555.98F);
        floatColumnVector.add(565.76F);
        floatColumnVector.add(234.11F);
        floatColumnVector.add(675.34F);
        floatColumnVector.add(235.58F);
        floatColumnVector.add(32434.68F);
        floatColumnVector.addNull();
        floatColumnVector.add(6.66F);
        floatColumnVector.add(7.77F);
        floatColumnVector.add(65656565.20F);
        floatColumnVector.add(3434.11F);
        floatColumnVector.add(54578.22F);
        columnWriter.write(floatColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        FloatColumnReader columnReader = new FloatColumnReader(TypeDescription.createFloat());
        FloatColumnVector floatColumnVector1 = new FloatColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, floatColumnVector1, chunkIndex);
        for (int i = 0; i < numRows; ++i)
        {
            assert floatColumnVector1.noNulls == floatColumnVector.noNulls;
            assert floatColumnVector1.isNull[i] == floatColumnVector.isNull[i];
            if (floatColumnVector.noNulls || !floatColumnVector.isNull[i])
            {
                assert floatColumnVector1.vector[i] == floatColumnVector.vector[i];
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
        FloatColumnWriter columnWriter = new FloatColumnWriter(
                TypeDescription.createFloat(), writerOption);
        FloatColumnVector floatColumnVector = new FloatColumnVector(numRows);
        floatColumnVector.add(100.22F);
        floatColumnVector.add(103.32F);
        floatColumnVector.add(106.43F);
        floatColumnVector.add(34.10F);
        floatColumnVector.addNull();
        floatColumnVector.add(54.09F);
        floatColumnVector.add(55.00F);
        floatColumnVector.add(67.23F);
        floatColumnVector.addNull();
        floatColumnVector.add(34.58F);
        floatColumnVector.add(555.98F);
        floatColumnVector.add(565.76F);
        floatColumnVector.add(234.11F);
        floatColumnVector.add(675.34F);
        floatColumnVector.add(235.58F);
        floatColumnVector.add(32434.68F);
        floatColumnVector.addNull();
        floatColumnVector.add(6.66F);
        floatColumnVector.add(7.77F);
        floatColumnVector.add(65656565.20F);
        floatColumnVector.add(3434.11F);
        floatColumnVector.add(54578.22F);
        columnWriter.write(floatColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        FloatColumnReader columnReader = new FloatColumnReader(TypeDescription.createFloat());
        FloatColumnVector floatColumnVector1 = new FloatColumnVector(numRows);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, floatColumnVector1, chunkIndex, selected);
        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assert floatColumnVector1.noNulls == floatColumnVector.noNulls;
                assert floatColumnVector1.isNull[j] == floatColumnVector.isNull[i];
                if (floatColumnVector.noNulls || !floatColumnVector.isNull[i])
                {
                    assert floatColumnVector1.vector[j] == floatColumnVector.vector[i];
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
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        FloatColumnWriter columnWriter = new FloatColumnWriter(
                TypeDescription.createFloat(), writerOption);

        FloatColumnVector originVector = new FloatColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000.0f);
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
        FloatColumnReader columnReader = new FloatColumnReader(TypeDescription.createFloat());
        FloatColumnVector targetVector = new FloatColumnVector(numBatches*numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numBatches*numRows,
                10000, 0, targetVector, chunkIndex);

        for (int i = 0; i < numBatches*numRows; i++)
        {
            assert targetVector.isNull[i] == originVector.isNull[i%numRows];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                assert targetVector.vector[i] == originVector.vector[i % numRows];
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
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        FloatColumnWriter columnWriter = new FloatColumnWriter(
                TypeDescription.createFloat(), writerOption);

        FloatColumnVector originVector = new FloatColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000.0f);
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
        FloatColumnReader columnReader = new FloatColumnReader(TypeDescription.createFloat());
        FloatColumnVector targetVector = new FloatColumnVector(numBatches*numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 123,
                10000, 0, targetVector, chunkIndex);
        columnReader.read(ByteBuffer.wrap(content), encoding, 123, 456,
                10000, 123, targetVector, chunkIndex);
        columnReader.read(ByteBuffer.wrap(content), encoding, 123+456, numBatches*numRows-123-456,
                10000, 123+456, targetVector, chunkIndex);

        for (int i = 0; i < numBatches*numRows; i++)
        {
            assert targetVector.isNull[i] == originVector.isNull[i%numRows];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                assert targetVector.vector[i] == originVector.vector[i % numRows];
            }
        }
    }
}
