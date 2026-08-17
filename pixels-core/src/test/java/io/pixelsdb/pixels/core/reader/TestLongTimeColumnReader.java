/*
 * Copyright 2026 PixelsDB.
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
import io.pixelsdb.pixels.core.vector.LongTimeColumnVector;
import io.pixelsdb.pixels.core.vector.TimeColumnVector;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.TimeColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.PICOS_PER_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link LongTimeColumnReader}: on-disk TIME (millis int) projected to
 * {@link LongTimeColumnVector} (picoseconds), selected by
 * {@link PixelsReaderOption#readTimeColumnAsLongTimeVector(boolean)}.
 *
 * @author gengdy
 * @create 2026-08-17
 */
public class TestLongTimeColumnReader
{
    private static TimeColumnVector createSampleMillisVector(int numRows)
    {
        TimeColumnVector vector = new TimeColumnVector(numRows, 3);
        vector.add(100);
        vector.add(103);
        vector.add(106);
        vector.add(34);
        vector.addNull();
        vector.add(54);
        vector.add(55);
        vector.add(67);
        vector.addNull();
        vector.add(34);
        vector.add(555);
        vector.add(565);
        vector.add(234);
        vector.add(675);
        vector.add(235);
        vector.add(32434);
        vector.addNull();
        vector.add(6);
        vector.add(7);
        vector.add(65656565);
        vector.add(3434);
        vector.add(54578);
        return vector;
    }

    private static void assertMillisProjectedToPicos(TimeColumnVector source,
                                                     LongTimeColumnVector target, int numRows)
    {
        assertEquals(source.noNulls, target.noNulls);
        for (int i = 0; i < numRows; ++i)
        {
            assertEquals("isNull mismatch at row " + i, source.isNull[i], target.isNull[i]);
            if (source.noNulls || !source.isNull[i])
            {
                assertEquals("pico mismatch at row " + i,
                        (long) source.times[i] * PICOS_PER_MILLIS, target.vector[i]);
            }
        }
    }

    @Test
    public void testFactorySelectsLongTimeColumnReader()
    {
        TypeDescription timeType = TypeDescription.createTime(3);
        assertTrue(ColumnReader.newColumnReader(timeType, new PixelsReaderOption())
                instanceof TimeColumnReader);
        assertTrue(ColumnReader.newColumnReader(timeType,
                new PixelsReaderOption().readTimeColumnAsLongTimeVector(true))
                instanceof LongTimeColumnReader);
    }

    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        TimeColumnVector source = createSampleMillisVector(numRows);
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        columnWriter.write(source, numRows);
        columnWriter.flush();
        columnWriter.close();

        LongTimeColumnVector target = new LongTimeColumnVector(numRows, 3);
        LongTimeColumnReader reader = new LongTimeColumnReader(TypeDescription.createTime(3));
        reader.read(ByteBuffer.wrap(columnWriter.getColumnChunkContent()),
                columnWriter.getColumnChunkEncoding().build(), 0, numRows,
                pixelsStride, 0, target, columnWriter.getColumnChunkIndex().build());
        reader.close();

        assertMillisProjectedToPicos(source, target, numRows);
    }

    @Test
    public void testWithoutNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        TimeColumnVector source = createSampleMillisVector(numRows);
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        columnWriter.write(source, numRows);
        columnWriter.flush();
        columnWriter.close();

        LongTimeColumnVector target = new LongTimeColumnVector(numRows, 3);
        LongTimeColumnReader reader = new LongTimeColumnReader(TypeDescription.createTime(3));
        reader.read(ByteBuffer.wrap(columnWriter.getColumnChunkContent()),
                columnWriter.getColumnChunkEncoding().build(), 0, numRows,
                pixelsStride, 0, target, columnWriter.getColumnChunkIndex().build());
        reader.close();

        assertMillisProjectedToPicos(source, target, numRows);
    }

    @Test
    public void testRunLengthEncoded() throws IOException
    {
        int pixelStride = 4;
        int[] millis = {0, 1, 3_723_004, 86_399_999};
        TimeColumnVector source = new TimeColumnVector(millis.length + 1, 3);
        for (int value : millis)
        {
            source.add(value);
        }
        source.addNull();

        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL2).nullsPadding(false);
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        columnWriter.write(source, millis.length + 1);
        columnWriter.flush();
        columnWriter.close();

        assertEquals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH,
                columnWriter.getColumnChunkEncoding().build().getKind());

        LongTimeColumnVector target = new LongTimeColumnVector(millis.length + 1, 3);
        LongTimeColumnReader reader = new LongTimeColumnReader(TypeDescription.createTime(3));
        reader.read(ByteBuffer.wrap(columnWriter.getColumnChunkContent()),
                columnWriter.getColumnChunkEncoding().build(), 0, millis.length + 1,
                pixelStride, 0, target, columnWriter.getColumnChunkIndex().build());
        reader.close();

        for (int i = 0; i < millis.length; ++i)
        {
            assertFalse(target.isNull[i]);
            assertEquals((long) millis[i] * PICOS_PER_MILLIS, target.vector[i]);
        }
        assertTrue(target.isNull[millis.length]);
    }

    @Test
    public void testSelectedWithNullPadding() throws IOException
    {
        int pixelStride = 4;
        int[] millis = {10, 20, 30, 40, 50};
        TimeColumnVector source = new TimeColumnVector(millis.length, 3);
        source.add(millis[0]);
        source.addNull();
        for (int i = 2; i < millis.length; ++i)
        {
            source.add(millis[i]);
        }

        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        columnWriter.write(source, millis.length);
        columnWriter.flush();
        columnWriter.close();

        Bitmap selected = new Bitmap(millis.length, true);
        selected.clear(0);
        selected.clear(3);
        LongTimeColumnVector target = new LongTimeColumnVector(3, 3);
        LongTimeColumnReader reader = new LongTimeColumnReader(TypeDescription.createTime(3));
        reader.readSelected(
                ByteBuffer.wrap(columnWriter.getColumnChunkContent()),
                columnWriter.getColumnChunkEncoding().build(),
                0, millis.length, pixelStride, 0, target,
                columnWriter.getColumnChunkIndex().build(), selected);
        reader.close();

        assertTrue(target.isNull[0]);
        assertEquals((long) millis[2] * PICOS_PER_MILLIS, target.vector[1]);
        assertEquals((long) millis[4] * PICOS_PER_MILLIS, target.vector[2]);
    }

    @Test
    public void testSelectedWithoutNullPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        TimeColumnVector source = createSampleMillisVector(numRows);
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        columnWriter.write(source, numRows);
        columnWriter.flush();
        columnWriter.close();

        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);

        LongTimeColumnVector target = new LongTimeColumnVector(numRows, 3);
        LongTimeColumnReader reader = new LongTimeColumnReader(TypeDescription.createTime(3));
        reader.readSelected(ByteBuffer.wrap(columnWriter.getColumnChunkContent()),
                columnWriter.getColumnChunkEncoding().build(), 0, numRows,
                pixelsStride, 0, target, columnWriter.getColumnChunkIndex().build(), selected);
        reader.close();

        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assertEquals(source.isNull[i], target.isNull[j]);
                if (source.noNulls || !source.isNull[i])
                {
                    assertEquals((long) source.times[i] * PICOS_PER_MILLIS, target.vector[j]);
                }
                j++;
            }
        }
    }

    @Test
    public void testVectorLayoutCreatesLongTimeColumnVector()
    {
        TypeDescription timeType = TypeDescription.createTime(3);
        assertTrue(timeType.createRowBatch(4, TypeDescription.VectorLayout.TIME_AS_LONG_TIME)
                .cols[0] instanceof LongTimeColumnVector);
        assertTrue(timeType.createRowBatch(4).cols[0] instanceof TimeColumnVector);
    }

    @Test
    public void testLargeFragmented() throws IOException
    {
        int numBatches = 15;
        int numRows = 1024;
        TimeColumnVector origin = new TimeColumnVector(numRows, 3);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                origin.addNull();
            }
            else
            {
                origin.add(1000);
            }
        }

        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10000).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        for (int i = 0; i < numBatches; i++)
        {
            columnWriter.write(origin, numRows);
        }
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        LongTimeColumnReader reader = new LongTimeColumnReader(TypeDescription.createTime(3));
        LongTimeColumnVector target = new LongTimeColumnVector(numBatches * numRows, 3);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        reader.read(buffer, encoding, 0, 123, 10000, 0, target, chunkIndex);
        reader.read(buffer, encoding, 123, 456, 10000, 123, target, chunkIndex);
        reader.read(buffer, encoding, 123 + 456, numBatches * numRows - 123 - 456,
                10000, 123 + 456, target, chunkIndex);
        reader.close();

        for (int i = 0; i < numBatches * numRows; i++)
        {
            assertEquals(origin.isNull[i % numRows], target.isNull[i]);
            if (target.noNulls || !target.isNull[i])
            {
                assertEquals((long) origin.times[i % numRows] * PICOS_PER_MILLIS,
                        target.vector[i]);
            }
        }
    }
}
