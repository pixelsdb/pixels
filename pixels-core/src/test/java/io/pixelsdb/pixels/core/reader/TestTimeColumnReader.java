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
import io.pixelsdb.pixels.core.vector.TimeColumnVector;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.TimeColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestTimeColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        TimeColumnVector timeColumnVector = new TimeColumnVector(numRows, 3);
        timeColumnVector.add(100);
        timeColumnVector.add(103);
        timeColumnVector.add(106);
        timeColumnVector.add(34);
        timeColumnVector.addNull();
        timeColumnVector.add(54);
        timeColumnVector.add(55);
        timeColumnVector.add(67);
        timeColumnVector.addNull();
        timeColumnVector.add(34);
        timeColumnVector.add(555);
        timeColumnVector.add(565);
        timeColumnVector.add(234);
        timeColumnVector.add(675);
        timeColumnVector.add(235);
        timeColumnVector.add(32434);
        timeColumnVector.addNull();
        timeColumnVector.add(6);
        timeColumnVector.add(7);
        timeColumnVector.add(65656565);
        timeColumnVector.add(3434);
        timeColumnVector.add(54578);
        columnWriter.write(timeColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimeColumnReader columnReader = new TimeColumnReader(TypeDescription.createTime(3));
        TimeColumnVector timeColumnVector1 = new TimeColumnVector(numRows, 3);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, timeColumnVector1, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert timeColumnVector1.noNulls == timeColumnVector.noNulls;
            assert timeColumnVector1.isNull[i] == timeColumnVector.isNull[i];
            if (timeColumnVector.noNulls || !timeColumnVector.isNull[i])
            {
                assert timeColumnVector1.times[i] == timeColumnVector.times[i];
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
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        TimeColumnVector timeColumnVector = new TimeColumnVector(numRows, 3);
        timeColumnVector.add(100);
        timeColumnVector.add(103);
        timeColumnVector.add(106);
        timeColumnVector.add(34);
        timeColumnVector.addNull();
        timeColumnVector.add(54);
        timeColumnVector.add(55);
        timeColumnVector.add(67);
        timeColumnVector.addNull();
        timeColumnVector.add(34);
        timeColumnVector.add(555);
        timeColumnVector.add(565);
        timeColumnVector.add(234);
        timeColumnVector.add(675);
        timeColumnVector.add(235);
        timeColumnVector.add(32434);
        timeColumnVector.addNull();
        timeColumnVector.add(6);
        timeColumnVector.add(7);
        timeColumnVector.add(65656565);
        timeColumnVector.add(3434);
        timeColumnVector.add(54578);
        columnWriter.write(timeColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimeColumnReader columnReader = new TimeColumnReader(TypeDescription.createTime(3));
        TimeColumnVector timeColumnVector1 = new TimeColumnVector(numRows, 3);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, timeColumnVector1, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert timeColumnVector1.noNulls == timeColumnVector.noNulls;
            assert timeColumnVector1.isNull[i] == timeColumnVector.isNull[i];
            if (timeColumnVector.noNulls || !timeColumnVector.isNull[i])
            {
                assert timeColumnVector1.times[i] == timeColumnVector.times[i];
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
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        TimeColumnVector timeColumnVector = new TimeColumnVector(numRows, 3);
        timeColumnVector.add(100);
        timeColumnVector.add(103);
        timeColumnVector.add(106);
        timeColumnVector.add(34);
        timeColumnVector.addNull();
        timeColumnVector.add(54);
        timeColumnVector.add(55);
        timeColumnVector.add(67);
        timeColumnVector.addNull();
        timeColumnVector.add(34);
        timeColumnVector.add(555);
        timeColumnVector.add(565);
        timeColumnVector.add(234);
        timeColumnVector.add(675);
        timeColumnVector.add(235);
        timeColumnVector.add(32434);
        timeColumnVector.addNull();
        timeColumnVector.add(6);
        timeColumnVector.add(7);
        timeColumnVector.add(65656565);
        timeColumnVector.add(3434);
        timeColumnVector.add(54578);
        columnWriter.write(timeColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimeColumnReader columnReader = new TimeColumnReader(TypeDescription.createTime(3));
        TimeColumnVector timeColumnVector1 = new TimeColumnVector(numRows, 3);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, timeColumnVector1, chunkIndex, selected);
        columnReader.close();

        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assert timeColumnVector1.noNulls == timeColumnVector.noNulls;
                assert timeColumnVector1.isNull[j] == timeColumnVector.isNull[i];
                if (timeColumnVector.noNulls || !timeColumnVector.isNull[i])
                {
                    assert timeColumnVector1.times[j] == timeColumnVector.times[i];
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
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);

        TimeColumnVector originVector = new TimeColumnVector(numRows, 3);
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
        TimeColumnReader columnReader = new TimeColumnReader(TypeDescription.createTime(3));
        TimeColumnVector targetVector = new TimeColumnVector(numBatches*numRows, 3);
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
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);

        TimeColumnVector originVector = new TimeColumnVector(numRows, 3);
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
        TimeColumnReader columnReader = new TimeColumnReader(TypeDescription.createTime(3));
        TimeColumnVector targetVector = new TimeColumnVector(numBatches*numRows, 3);
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
