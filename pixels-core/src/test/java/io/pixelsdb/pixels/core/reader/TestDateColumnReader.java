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
import io.pixelsdb.pixels.core.vector.DateColumnVector;
import io.pixelsdb.pixels.core.writer.DateColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-19 Zermatt
 */
public class TestDateColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        DateColumnWriter columnWriter = new DateColumnWriter(
                TypeDescription.createDate(), writerOption);
        DateColumnVector dateColumnVector = new DateColumnVector(numRows);
        dateColumnVector.add(100);
        dateColumnVector.add(103);
        dateColumnVector.add(106);
        dateColumnVector.add(34);
        dateColumnVector.addNull();
        dateColumnVector.add(54);
        dateColumnVector.add(55);
        dateColumnVector.add(67);
        dateColumnVector.addNull();
        dateColumnVector.add(34);
        dateColumnVector.add(555);
        dateColumnVector.add(565);
        dateColumnVector.add(234);
        dateColumnVector.add(675);
        dateColumnVector.add(235);
        dateColumnVector.add(32434);
        dateColumnVector.addNull();
        dateColumnVector.add(6);
        dateColumnVector.add(7);
        dateColumnVector.add(65656565);
        dateColumnVector.add(3434);
        dateColumnVector.add(54578);
        columnWriter.write(dateColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DateColumnReader columnReader = new DateColumnReader(TypeDescription.createDate());
        DateColumnVector dateColumnVector1 = new DateColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, dateColumnVector1, chunkIndex);
        for (int i = 0; i < numRows; ++i)
        {
            assert dateColumnVector1.noNulls == dateColumnVector.noNulls;
            assert dateColumnVector1.isNull[i] == dateColumnVector.isNull[i];
            if (dateColumnVector.noNulls || !dateColumnVector.isNull[i])
            {
                assert dateColumnVector1.dates[i] == dateColumnVector.dates[i];
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
        DateColumnWriter columnWriter = new DateColumnWriter(
                TypeDescription.createDate(), writerOption);
        DateColumnVector dateColumnVector = new DateColumnVector(numRows);
        dateColumnVector.add(100);
        dateColumnVector.add(103);
        dateColumnVector.add(106);
        dateColumnVector.add(34);
        dateColumnVector.addNull();
        dateColumnVector.add(54);
        dateColumnVector.add(55);
        dateColumnVector.add(67);
        dateColumnVector.addNull();
        dateColumnVector.add(34);
        dateColumnVector.add(555);
        dateColumnVector.add(565);
        dateColumnVector.add(234);
        dateColumnVector.add(675);
        dateColumnVector.add(235);
        dateColumnVector.add(32434);
        dateColumnVector.addNull();
        dateColumnVector.add(6);
        dateColumnVector.add(7);
        dateColumnVector.add(65656565);
        dateColumnVector.add(3434);
        dateColumnVector.add(54578);
        columnWriter.write(dateColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DateColumnReader columnReader = new DateColumnReader(TypeDescription.createDate());
        DateColumnVector dateColumnVector1 = new DateColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, dateColumnVector1, chunkIndex);
        for (int i = 0; i < numRows; ++i)
        {
            assert dateColumnVector1.noNulls == dateColumnVector.noNulls;
            assert dateColumnVector1.isNull[i] == dateColumnVector.isNull[i];
            if (dateColumnVector.noNulls || !dateColumnVector.isNull[i])
            {
                assert dateColumnVector1.dates[i] == dateColumnVector.dates[i];
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
        DateColumnWriter columnWriter = new DateColumnWriter(
                TypeDescription.createDate(), writerOption);
        DateColumnVector dateColumnVector = new DateColumnVector(numRows);
        dateColumnVector.add(100);
        dateColumnVector.add(103);
        dateColumnVector.add(106);
        dateColumnVector.add(34);
        dateColumnVector.addNull();
        dateColumnVector.add(54);
        dateColumnVector.add(55);
        dateColumnVector.add(67);
        dateColumnVector.addNull();
        dateColumnVector.add(34);
        dateColumnVector.add(555);
        dateColumnVector.add(565);
        dateColumnVector.add(234);
        dateColumnVector.add(675);
        dateColumnVector.add(235);
        dateColumnVector.add(32434);
        dateColumnVector.addNull();
        dateColumnVector.add(6);
        dateColumnVector.add(7);
        dateColumnVector.add(65656565);
        dateColumnVector.add(3434);
        dateColumnVector.add(54578);
        columnWriter.write(dateColumnVector, 22);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DateColumnReader columnReader = new DateColumnReader(TypeDescription.createDate());
        DateColumnVector dateColumnVector1 = new DateColumnVector(numRows);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, dateColumnVector1, chunkIndex, selected);
        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assert dateColumnVector1.noNulls == dateColumnVector.noNulls;
                assert dateColumnVector1.isNull[j] == dateColumnVector.isNull[i];
                if (dateColumnVector.noNulls || !dateColumnVector.isNull[i])
                {
                    assert dateColumnVector1.dates[j] == dateColumnVector.dates[i];
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
        DateColumnWriter columnWriter = new DateColumnWriter(
                TypeDescription.createDate(), writerOption);

        DateColumnVector originVector = new DateColumnVector(numRows);
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
        DateColumnReader columnReader = new DateColumnReader(TypeDescription.createDate());
        DateColumnVector targetVector = new DateColumnVector(numBatches*numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numBatches*numRows,
                10000, 0, targetVector, chunkIndex);

        for (int i = 0; i < numBatches*numRows; i++)
        {
            assert targetVector.isNull[i] == originVector.isNull[i%numRows];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                assert targetVector.dates[i] == originVector.dates[i % numRows];
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
        DateColumnWriter columnWriter = new DateColumnWriter(
                TypeDescription.createDate(), writerOption);

        DateColumnVector originVector = new DateColumnVector(numRows);
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
        DateColumnReader columnReader = new DateColumnReader(TypeDescription.createDate());
        DateColumnVector targetVector = new DateColumnVector(numBatches*numRows);
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
                assert targetVector.dates[i] == originVector.dates[i % numRows];
            }
        }
    }
}
