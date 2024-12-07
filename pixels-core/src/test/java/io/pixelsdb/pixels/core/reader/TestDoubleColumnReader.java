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
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.writer.DoubleColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestDoubleColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        DoubleColumnWriter columnWriter = new DoubleColumnWriter(
                TypeDescription.createDouble(), writerOption);
        DoubleColumnVector doubleColumnVector = new DoubleColumnVector(numRows);
        doubleColumnVector.add(100.22);
        doubleColumnVector.add(103.32);
        doubleColumnVector.add(106.43);
        doubleColumnVector.add(34.10);
        doubleColumnVector.addNull();
        doubleColumnVector.add(54.09);
        doubleColumnVector.add(55.00);
        doubleColumnVector.add(67.23);
        doubleColumnVector.addNull();
        doubleColumnVector.add(34.58);
        doubleColumnVector.add(555.98);
        doubleColumnVector.add(565.76);
        doubleColumnVector.add(234.11);
        doubleColumnVector.add(675.34);
        doubleColumnVector.add(235.58);
        doubleColumnVector.add(32434.68);
        doubleColumnVector.addNull(); // or add(3.58);
        doubleColumnVector.add(6.66);
        doubleColumnVector.add(7.77);
        doubleColumnVector.add(65656565.20);
        doubleColumnVector.add(3434.11);
        doubleColumnVector.add(54578.22);
        columnWriter.write(doubleColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DoubleColumnReader columnReader = new DoubleColumnReader(TypeDescription.createDouble());
        DoubleColumnVector doubleColumnVector1 = new DoubleColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, doubleColumnVector1, chunkIndex);
        for (int i = 0; i < numRows; ++i)
        {
            assert doubleColumnVector1.noNulls == doubleColumnVector.noNulls;
            assert doubleColumnVector1.isNull[i] == doubleColumnVector.isNull[i];
            if (doubleColumnVector.noNulls || !doubleColumnVector.isNull[i])
            {
                assert doubleColumnVector1.vector[i] == doubleColumnVector.vector[i];
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
        DoubleColumnWriter columnWriter = new DoubleColumnWriter(
                TypeDescription.createDouble(), writerOption);
        DoubleColumnVector doubleColumnVector = new DoubleColumnVector(numRows);
        doubleColumnVector.add(100.22);
        doubleColumnVector.add(103.32);
        doubleColumnVector.add(106.43);
        doubleColumnVector.add(34.10);
        doubleColumnVector.addNull();
        doubleColumnVector.add(54.09);
        doubleColumnVector.add(55.00);
        doubleColumnVector.add(67.23);
        doubleColumnVector.addNull();
        doubleColumnVector.add(34.58);
        doubleColumnVector.add(555.98);
        doubleColumnVector.add(565.76);
        doubleColumnVector.add(234.11);
        doubleColumnVector.add(675.34);
        doubleColumnVector.add(235.58);
        doubleColumnVector.add(32434.68);
        doubleColumnVector.addNull();
        doubleColumnVector.add(6.66);
        doubleColumnVector.add(7.77);
        doubleColumnVector.add(65656565.20);
        doubleColumnVector.add(3434.11);
        doubleColumnVector.add(54578.22);
        columnWriter.write(doubleColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DoubleColumnReader columnReader = new DoubleColumnReader(TypeDescription.createDouble());
        DoubleColumnVector doubleColumnVector1 = new DoubleColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, doubleColumnVector1, chunkIndex);
        for (int i = 0; i < numRows; ++i)
        {
            assert doubleColumnVector1.noNulls == doubleColumnVector.noNulls;
            assert doubleColumnVector1.isNull[i] == doubleColumnVector.isNull[i];
            if (doubleColumnVector.noNulls || !doubleColumnVector.isNull[i])
            {
                assert doubleColumnVector1.vector[i] == doubleColumnVector.vector[i];
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
        DoubleColumnWriter columnWriter = new DoubleColumnWriter(
                TypeDescription.createDouble(), writerOption);
        DoubleColumnVector doubleColumnVector = new DoubleColumnVector(numRows);
        doubleColumnVector.add(100.22);
        doubleColumnVector.add(103.32);
        doubleColumnVector.add(106.43);
        doubleColumnVector.add(34.10);
        doubleColumnVector.addNull();
        doubleColumnVector.add(54.09);
        doubleColumnVector.add(55.00);
        doubleColumnVector.add(67.23);
        doubleColumnVector.addNull();
        doubleColumnVector.add(34.58);
        doubleColumnVector.add(555.98);
        doubleColumnVector.add(565.76);
        doubleColumnVector.add(234.11);
        doubleColumnVector.add(675.34);
        doubleColumnVector.add(235.58);
        doubleColumnVector.add(32434.68);
        doubleColumnVector.addNull();
        doubleColumnVector.add(6.66);
        doubleColumnVector.add(7.77);
        doubleColumnVector.add(65656565.20);
        doubleColumnVector.add(3434.11);
        doubleColumnVector.add(54578.22);
        columnWriter.write(doubleColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DoubleColumnReader columnReader = new DoubleColumnReader(TypeDescription.createDouble());
        DoubleColumnVector doubleColumnVector1 = new DoubleColumnVector(numRows);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, doubleColumnVector1, chunkIndex, selected);
        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assert doubleColumnVector1.noNulls == doubleColumnVector.noNulls;
                assert doubleColumnVector1.isNull[j] == doubleColumnVector.isNull[i];
                if (doubleColumnVector.noNulls || !doubleColumnVector.isNull[i])
                {
                    assert doubleColumnVector1.vector[j] == doubleColumnVector.vector[i];
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
        DoubleColumnWriter columnWriter = new DoubleColumnWriter(
                TypeDescription.createDouble(), writerOption);

        DoubleColumnVector originVector = new DoubleColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000.0d);
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
        DoubleColumnReader columnReader = new DoubleColumnReader(TypeDescription.createDouble());
        DoubleColumnVector targetVector = new DoubleColumnVector(numBatches*numRows);
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
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        DoubleColumnWriter columnWriter = new DoubleColumnWriter(
                TypeDescription.createDouble(), writerOption);

        DoubleColumnVector originVector = new DoubleColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000.0d);
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
        DoubleColumnReader columnReader = new DoubleColumnReader(TypeDescription.createDouble());
        DoubleColumnVector targetVector = new DoubleColumnVector(numBatches*numRows);
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
