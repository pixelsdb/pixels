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
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.writer.BooleanColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-18
 */
public class TestBooleanColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        BooleanColumnWriter columnWriter = new BooleanColumnWriter(
                TypeDescription.createBoolean(), writerOption);
        ByteColumnVector byteColumnVector = new ByteColumnVector(numRows);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        columnWriter.write(byteColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        BooleanColumnReader columnReader = new BooleanColumnReader(TypeDescription.createBoolean());
        ByteColumnVector byteColumnVector1 = new ByteColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, byteColumnVector1, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert byteColumnVector1.noNulls == byteColumnVector.noNulls;
            assert byteColumnVector1.isNull[i] == byteColumnVector.isNull[i];
            if (byteColumnVector.noNulls || !byteColumnVector.isNull[i])
            {
                assert byteColumnVector1.vector[i] == byteColumnVector.vector[i];
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
        BooleanColumnWriter columnWriter = new BooleanColumnWriter(
                TypeDescription.createBoolean(), writerOption);
        ByteColumnVector byteColumnVector = new ByteColumnVector(numRows);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        columnWriter.write(byteColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        BooleanColumnReader columnReader = new BooleanColumnReader(TypeDescription.createBoolean());
        ByteColumnVector byteColumnVector1 = new ByteColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, byteColumnVector1, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert byteColumnVector1.noNulls == byteColumnVector.noNulls;
            assert byteColumnVector1.isNull[i] == byteColumnVector.isNull[i];
            if (byteColumnVector.noNulls || !byteColumnVector.isNull[i])
            {
                assert byteColumnVector1.vector[i] == byteColumnVector.vector[i];
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
        BooleanColumnWriter columnWriter = new BooleanColumnWriter(
                TypeDescription.createBoolean(), writerOption);
        ByteColumnVector byteColumnVector = new ByteColumnVector(numRows);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        columnWriter.write(byteColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        BooleanColumnReader columnReader = new BooleanColumnReader(TypeDescription.createBoolean());
        ByteColumnVector byteColumnVector1 = new ByteColumnVector(numRows);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, byteColumnVector1, chunkIndex, selected);
        columnReader.close();

        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assert byteColumnVector1.noNulls == byteColumnVector.noNulls;
                assert byteColumnVector1.isNull[j] == byteColumnVector.isNull[i];
                if (byteColumnVector.noNulls || !byteColumnVector.isNull[i])
                {
                    assert byteColumnVector1.vector[j] == byteColumnVector.vector[i];
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
        BooleanColumnWriter columnWriter = new BooleanColumnWriter(
                TypeDescription.createBoolean(), writerOption);

        ByteColumnVector originVector = new ByteColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(true);
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
        BooleanColumnReader columnReader = new BooleanColumnReader(TypeDescription.createBoolean());
        ByteColumnVector targetVector = new ByteColumnVector(numBatches*numRows);
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
        BooleanColumnWriter columnWriter = new BooleanColumnWriter(
                TypeDescription.createBoolean(), writerOption);

        ByteColumnVector originVector = new ByteColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(true);
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
        BooleanColumnReader columnReader = new BooleanColumnReader(TypeDescription.createBoolean());
        ByteColumnVector targetVector = new ByteColumnVector(numBatches*numRows);
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
