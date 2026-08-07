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
 * You should have received a copy of the GNU Affero General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.ShortColumnVector;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.ShortColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;

/**
 * @author gengdy
 * @create 2026-08-07
 */
public class TestShortColumnReader
{
    private static ShortColumnVector createSampleVector(int numRows)
    {
        ShortColumnVector vector = new ShortColumnVector(numRows);
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
        vector.add(Short.MAX_VALUE);
        vector.add(3434);
        vector.add(Short.MIN_VALUE);
        return vector;
    }

    private static void assertVectorsEqual(ShortColumnVector expected, ShortColumnVector actual, int numRows)
    {
        assertEquals(expected.noNulls, actual.noNulls);
        for (int i = 0; i < numRows; ++i)
        {
            assertEquals("isNull mismatch at row " + i, expected.isNull[i], actual.isNull[i]);
            if (expected.noNulls || !expected.isNull[i])
            {
                assertEquals("value mismatch at row " + i, expected.vector[i], actual.vector[i]);
            }
        }
    }

    private static void assertSelectedRoundTrip(EncodingLevel encodingLevel, boolean nullsPadding,
                                                PixelsProto.ColumnEncoding.Kind expectedEncoding)
            throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        int vectorIndex = 3;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(encodingLevel).nullsPadding(nullsPadding);
        ShortColumnWriter columnWriter = new ShortColumnWriter(
                TypeDescription.createShort(), writerOption);
        ShortColumnVector originVector = createSampleVector(numRows);
        columnWriter.write(originVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(expectedEncoding, encoding.getKind());

        Bitmap selected = new Bitmap(numRows, true);
        // Skip non-null and null rows so the reader must still consume encoded payload.
        selected.clear(0);
        selected.clear(2);
        selected.clear(4);
        selected.clear(5);
        selected.clear(10);
        selected.clear(14);
        selected.clear(16);
        selected.clear(20);

        ShortColumnReader columnReader = new ShortColumnReader(TypeDescription.createShort());
        ShortColumnVector targetVector = new ShortColumnVector(vectorIndex + numRows);
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
                    assertEquals("value mismatch at selected src=" + i + " dst=" + targetIndex,
                            originVector.vector[i], targetVector.vector[targetIndex]);
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
        ShortColumnWriter columnWriter = new ShortColumnWriter(
                TypeDescription.createShort(), writerOption);
        ShortColumnVector sourceVector = createSampleVector(numRows);
        columnWriter.write(sourceVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        ShortColumnReader columnReader = new ShortColumnReader(TypeDescription.createShort());
        ShortColumnVector targetVector = new ShortColumnVector(numRows);
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
        ShortColumnWriter columnWriter = new ShortColumnWriter(
                TypeDescription.createShort(), writerOption);
        ShortColumnVector sourceVector = createSampleVector(numRows);
        columnWriter.write(sourceVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        ShortColumnReader columnReader = new ShortColumnReader(TypeDescription.createShort());
        ShortColumnVector targetVector = new ShortColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, targetVector, chunkIndex);
        columnReader.close();

        assertVectorsEqual(sourceVector, targetVector, numRows);
    }

    @Test
    public void testRunLength() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL2).nullsPadding(false);
        ShortColumnWriter columnWriter = new ShortColumnWriter(
                TypeDescription.createShort(), writerOption);
        ShortColumnVector originVector = new ShortColumnVector(numRows);
        originVector.add(5);
        originVector.add(5);
        originVector.add(5);
        originVector.add(5);
        originVector.addNull();
        originVector.add(5);
        originVector.add(5);
        originVector.add(7);
        originVector.addNull();
        originVector.add(7);
        originVector.add(1);
        originVector.add(2);
        originVector.add(3);
        originVector.add(9);
        originVector.add(9);
        originVector.add(9);
        originVector.addNull();
        originVector.add(9);
        originVector.add(9);
        originVector.add(Short.MIN_VALUE);
        originVector.add(Short.MAX_VALUE);
        originVector.add(0);
        columnWriter.write(originVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH, encoding.getKind());
        ShortColumnReader columnReader = new ShortColumnReader(TypeDescription.createShort());
        ShortColumnVector targetVector = new ShortColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, targetVector, chunkIndex);
        columnReader.close();

        assertVectorsEqual(originVector, targetVector, numRows);
    }

    @Test
    public void testSelected() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        ShortColumnWriter columnWriter = new ShortColumnWriter(
                TypeDescription.createShort(), writerOption);
        ShortColumnVector sourceVector = createSampleVector(numRows);
        columnWriter.write(sourceVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        ShortColumnReader columnReader = new ShortColumnReader(TypeDescription.createShort());
        ShortColumnVector targetVector = new ShortColumnVector(numRows);
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
                    assertEquals(sourceVector.vector[i], targetVector.vector[j]);
                }
                j++;
            }
        }
    }

    @Test
    public void testSelectedWithoutNullsPaddingAtNonZeroVectorIndex() throws IOException
    {
        assertSelectedRoundTrip(EncodingLevel.EL0, false, PixelsProto.ColumnEncoding.Kind.NONE);
    }

    @Test
    public void testSelectedRunLengthAtNonZeroVectorIndex() throws IOException
    {
        assertSelectedRoundTrip(EncodingLevel.EL2, false, PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
    }

    @Test
    public void testLargeFragmented() throws IOException
    {
        int numBatches = 15;
        int numRows = 1024;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10000).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL2).nullsPadding(false);
        ShortColumnWriter columnWriter = new ShortColumnWriter(
                TypeDescription.createShort(), writerOption);

        ShortColumnVector originVector = new ShortColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add((short) ((j / 200) % 4));
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
        assertEquals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH, encoding.getKind());
        int totalRows = numBatches * numRows;
        ShortColumnReader columnReader = new ShortColumnReader(TypeDescription.createShort());
        ShortColumnVector targetVector = new ShortColumnVector(totalRows);
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
            assertEquals(originVector.isNull[i % numRows], targetVector.isNull[i]);
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                assertEquals(originVector.vector[i % numRows], targetVector.vector[i]);
            }
        }
    }
}
