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
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.writer.CharColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;

/**
 * Memory round-trip tests for CHAR. CharColumnWriter does not pad with trailing zeros;
 * values longer than maxLength are truncated by VarcharColumnWriter before encoding.
 *
 * @author gengdy
 * @create 2026-08-07
 */
public class TestCharColumnReader
{
    private static final int CHAR_MAX_LENGTH = 8;

    private static TypeDescription charType()
    {
        return TypeDescription.createChar(CHAR_MAX_LENGTH);
    }

    private static BinaryColumnVector createSampleVector(int numRows)
    {
        BinaryColumnVector vector = new BinaryColumnVector(numRows);
        // All non-null values are within CHAR_MAX_LENGTH; include empty and max-length boundary.
        vector.add("");
        vector.add("a");
        vector.add("ab");
        vector.add("abcdefg");
        vector.addNull();
        vector.add("abcdefgh"); // exactly maxLength
        vector.add("xy");
        vector.add("z");
        vector.addNull();
        vector.add("bound");
        vector.add("555");
        vector.add("565");
        vector.add("234");
        vector.add("675");
        vector.add("235");
        vector.add("32434"); // length 5
        vector.addNull();
        vector.add("6");
        vector.add("7");
        vector.add("maxchar!"); // exactly 8
        vector.add("3434");
        vector.add("end");
        return vector;
    }

    private static void assertVectorsEqual(BinaryColumnVector expected, BinaryColumnVector actual, int numRows)
    {
        assertEquals(expected.noNulls, actual.noNulls);
        for (int i = 0; i < numRows; ++i)
        {
            assertEquals("isNull mismatch at row " + i, expected.isNull[i], actual.isNull[i]);
            if (expected.noNulls || !expected.isNull[i])
            {
                String e = new String(expected.vector[i], expected.start[i], expected.lens[i]);
                String a = new String(actual.vector[i], actual.start[i], actual.lens[i]);
                assertEquals("value mismatch at row " + i, e, a);
            }
        }
    }

    private static void assertSelectedRoundTrip(boolean nullsPadding) throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        int vectorIndex = 3;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(nullsPadding);
        CharColumnWriter columnWriter = new CharColumnWriter(charType(), writerOption);
        BinaryColumnVector originVector = createSampleVector(numRows);
        columnWriter.write(originVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());

        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(2);
        selected.clear(4);
        selected.clear(5);
        selected.clear(10);
        selected.clear(14);
        selected.clear(16);
        selected.clear(20);

        CharColumnReader columnReader = new CharColumnReader(charType());
        BinaryColumnVector targetVector = new BinaryColumnVector(vectorIndex + numRows);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, vectorIndex, targetVector, chunkIndex, selected);
        columnReader.close();

        int targetIndex = vectorIndex;
        for (int i = 0; i < numRows; ++i)
        {
            if (selected.get(i))
            {
                assertEquals("isNull mismatch at selected src=" + i + " dst=" + targetIndex,
                        originVector.isNull[i], targetVector.isNull[targetIndex]);
                if (!originVector.isNull[i])
                {
                    String e = new String(originVector.vector[i], originVector.start[i], originVector.lens[i]);
                    String a = new String(targetVector.vector[targetIndex],
                            targetVector.start[targetIndex], targetVector.lens[targetIndex]);
                    assertEquals("value mismatch at selected src=" + i + " dst=" + targetIndex, e, a);
                }
                targetIndex++;
            }
        }
    }

    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        CharColumnWriter columnWriter = new CharColumnWriter(charType(), writerOption);
        BinaryColumnVector sourceVector = createSampleVector(numRows);
        columnWriter.write(sourceVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        CharColumnReader columnReader = new CharColumnReader(charType());
        BinaryColumnVector targetVector = new BinaryColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, targetVector, chunkIndex);
        columnReader.close();

        assertVectorsEqual(sourceVector, targetVector, numRows);
    }

    @Test
    public void testWithoutNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        CharColumnWriter columnWriter = new CharColumnWriter(charType(), writerOption);
        BinaryColumnVector sourceVector = createSampleVector(numRows);
        columnWriter.write(sourceVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        CharColumnReader columnReader = new CharColumnReader(charType());
        BinaryColumnVector targetVector = new BinaryColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, targetVector, chunkIndex);
        columnReader.close();

        assertVectorsEqual(sourceVector, targetVector, numRows);
    }

    @Test
    public void testSelected() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        CharColumnWriter columnWriter = new CharColumnWriter(charType(), writerOption);
        BinaryColumnVector sourceVector = createSampleVector(numRows);
        columnWriter.write(sourceVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        CharColumnReader columnReader = new CharColumnReader(charType());
        BinaryColumnVector targetVector = new BinaryColumnVector(numRows);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, targetVector, chunkIndex, selected);
        columnReader.close();

        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assertEquals(sourceVector.isNull[i], targetVector.isNull[j]);
                if (sourceVector.noNulls || !sourceVector.isNull[i])
                {
                    String e = new String(sourceVector.vector[i], sourceVector.start[i], sourceVector.lens[i]);
                    String a = new String(targetVector.vector[j], targetVector.start[j], targetVector.lens[j]);
                    assertEquals(e, a);
                }
                j++;
            }
        }
    }

    @Test
    public void testSelectedWithoutNullsPaddingAtNonZeroVectorIndex() throws IOException
    {
        assertSelectedRoundTrip(false);
    }

    @Test
    public void testSelectedWithNullsPaddingAtNonZeroVectorIndex() throws IOException
    {
        assertSelectedRoundTrip(true);
    }

    @Test
    public void testLargeFragmented() throws IOException
    {
        int numBatches = 15;
        int numRows = 1024;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10000).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        CharColumnWriter columnWriter = new CharColumnWriter(charType(), writerOption);

        BinaryColumnVector originVector = new BinaryColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                // Keep within CHAR_MAX_LENGTH.
                originVector.add("v" + (j % 10000));
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
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        int totalRows = numBatches * numRows;
        CharColumnReader columnReader = new CharColumnReader(charType());
        BinaryColumnVector targetVector = new BinaryColumnVector(totalRows);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        columnReader.read(buffer, encoding, 0, 123,
                10000, 0, targetVector, chunkIndex);
        columnReader.read(buffer, encoding, 123, 456,
                10000, 123, targetVector, chunkIndex);
        columnReader.read(buffer, encoding, 123 + 456, totalRows - 123 - 456,
                10000, 123 + 456, targetVector, chunkIndex);
        columnReader.close();

        for (int i = 0; i < totalRows; i++)
        {
            int j = i % numRows;
            assertEquals(originVector.isNull[j], targetVector.isNull[i]);
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                String e = new String(originVector.vector[j], originVector.start[j], originVector.lens[j]);
                String a = new String(targetVector.vector[i], targetVector.start[i], targetVector.lens[i]);
                assertEquals(e, a);
            }
        }
    }
}
