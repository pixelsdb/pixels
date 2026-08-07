/*
 * Copyright 2024 PixelsDB.
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
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.writer.IntColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2024-12-07
 */
public class TestIntColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        IntColumnWriter columnWriter = new IntColumnWriter(
                TypeDescription.createInt(), writerOption);
        IntColumnVector sourceVector = new IntColumnVector(numRows);
        sourceVector.add(100);
        sourceVector.add(103);
        sourceVector.add(106);
        sourceVector.add(34);
        sourceVector.addNull();
        sourceVector.add(54);
        sourceVector.add(55);
        sourceVector.add(67);
        sourceVector.addNull();
        sourceVector.add(34);
        sourceVector.add(555);
        sourceVector.add(565);
        sourceVector.add(234);
        sourceVector.add(675);
        sourceVector.add(235);
        sourceVector.add(32434);
        sourceVector.addNull();
        sourceVector.add(6);
        sourceVector.add(7);
        sourceVector.add(65656565);
        sourceVector.add(3434);
        sourceVector.add(54578);
        columnWriter.write(sourceVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        IntColumnReader columnReader = new IntColumnReader(TypeDescription.createInt());
        IntColumnVector intColumnVector = new IntColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, intColumnVector, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert intColumnVector.noNulls == sourceVector.noNulls;
            assert intColumnVector.isNull[i] == sourceVector.isNull[i];
            if (sourceVector.noNulls || !sourceVector.isNull[i])
            {
                assert intColumnVector.vector[i] == sourceVector.vector[i];
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
        IntColumnWriter columnWriter = new IntColumnWriter(
                TypeDescription.createInt(), writerOption);
        IntColumnVector sourceVector = new IntColumnVector(numRows);
        sourceVector.add(100);
        sourceVector.add(103);
        sourceVector.add(106);
        sourceVector.add(34);
        sourceVector.addNull();
        sourceVector.add(54);
        sourceVector.add(55);
        sourceVector.add(67);
        sourceVector.addNull();
        sourceVector.add(34);
        sourceVector.add(555);
        sourceVector.add(565);
        sourceVector.add(234);
        sourceVector.add(675);
        sourceVector.add(235);
        sourceVector.add(32434);
        sourceVector.addNull();
        sourceVector.add(6);
        sourceVector.add(7);
        sourceVector.add(65656565);
        sourceVector.add(3434);
        sourceVector.add(54578);
        columnWriter.write(sourceVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        IntColumnReader columnReader = new IntColumnReader(TypeDescription.createInt());
        IntColumnVector intColumnVector = new IntColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, intColumnVector, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert intColumnVector.noNulls == sourceVector.noNulls;
            assert intColumnVector.isNull[i] == sourceVector.isNull[i];
            if (sourceVector.noNulls || !sourceVector.isNull[i])
            {
                assert intColumnVector.vector[i] == sourceVector.vector[i];
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
        IntColumnWriter columnWriter = new IntColumnWriter(
                TypeDescription.createInt(), writerOption);
        IntColumnVector sourceVector = new IntColumnVector(numRows);
        sourceVector.add(100);
        sourceVector.add(103);
        sourceVector.add(106);
        sourceVector.add(34);
        sourceVector.addNull();
        sourceVector.add(54);
        sourceVector.add(55);
        sourceVector.add(67);
        sourceVector.addNull();
        sourceVector.add(34);
        sourceVector.add(555);
        sourceVector.add(565);
        sourceVector.add(234);
        sourceVector.add(675);
        sourceVector.add(235);
        sourceVector.add(32434);
        sourceVector.addNull();
        sourceVector.add(6);
        sourceVector.add(7);
        sourceVector.add(65656565);
        sourceVector.add(3434);
        sourceVector.add(54578);
        columnWriter.write(sourceVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        IntColumnReader columnReader = new IntColumnReader(TypeDescription.createInt());
        IntColumnVector intColumnVector = new IntColumnVector(numRows);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, intColumnVector, chunkIndex, selected);
        columnReader.close();

        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assert intColumnVector.noNulls == sourceVector.noNulls;
                assert intColumnVector.isNull[j] == sourceVector.isNull[i];
                if (sourceVector.noNulls || !sourceVector.isNull[i])
                {
                    assert intColumnVector.vector[j] == sourceVector.vector[i];
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
        IntColumnWriter columnWriter = new IntColumnWriter(
                TypeDescription.createInt(), writerOption);

        IntColumnVector originVector = new IntColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000);
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
        IntColumnReader columnReader = new IntColumnReader(TypeDescription.createInt());
        IntColumnVector targetVector = new IntColumnVector(numBatches*numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numBatches*numRows,
                10000, 0, targetVector, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numBatches*numRows; i++)
        {
            assert targetVector.isNull[i] == originVector.isNull[i%numRows];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                assert targetVector.vector[i] == originVector.vector[i % numRows];
            }
        }
    }

    /**
     * Test reading into column vectors with a run-length smaller than pixels stride.
     */
    @Test
    public void testLargeFragmented() throws IOException
    {
        int numBatches = 15;
        int numRows = 1024;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10000).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        IntColumnWriter columnWriter = new IntColumnWriter(
                TypeDescription.createInt(), writerOption);

        IntColumnVector originVector = new IntColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000);
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
        IntColumnReader columnReader = new IntColumnReader(TypeDescription.createInt());
        IntColumnVector targetVector = new IntColumnVector(numBatches*numRows);
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
                assert targetVector.vector[i] == originVector.vector[i % numRows];
            }
        }
    }
}
