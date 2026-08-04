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
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.writer.ByteColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;

/**
 * @author hank
 */
public class TestByteColumnReader
{
    private static String formatCell(ByteColumnVector vector, int i)
    {
        if (!vector.noNulls && vector.isNull[i])
        {
            return "NULL";
        }
        return Byte.toString(vector.vector[i]);
    }

    private static void printVector(String label, ByteColumnVector vector, int numRows)
    {
        StringBuilder sb = new StringBuilder(label).append(" [");
        for (int i = 0; i < numRows; ++i)
        {
            if (i > 0)
            {
                sb.append(", ");
            }
            sb.append(formatCell(vector, i));
        }
        sb.append(']');
        System.out.println(sb);
    }

    private static void assertVectorsEqual(String caseName, ByteColumnVector expected,
                                           ByteColumnVector actual, int numRows)
    {
        System.out.println("--- " + caseName + " ---");
        System.out.println("rows: " + numRows + ", expected.noNulls=" + expected.noNulls
                + ", actual.noNulls=" + actual.noNulls);
        printVector("expected", expected, numRows);
        printVector("actual  ", actual, numRows);
        assertEquals(expected.noNulls, actual.noNulls);
        for (int i = 0; i < numRows; ++i)
        {
            assertEquals("isNull mismatch at row " + i, expected.isNull[i], actual.isNull[i]);
            if (expected.noNulls || !expected.isNull[i])
            {
                assertEquals("value mismatch at row " + i, expected.vector[i], actual.vector[i]);
            }
        }
        System.out.println("OK: all " + numRows + " rows match");
    }

    private static ByteColumnVector createSampleVector(int numRows)
    {
        ByteColumnVector vector = new ByteColumnVector(numRows);
        vector.add((byte) 100);
        vector.add((byte) 103);
        vector.add((byte) 106);
        vector.add((byte) 34);
        vector.addNull();
        vector.add((byte) 54);
        vector.add((byte) 55);
        vector.add((byte) 67);
        vector.addNull();
        vector.add((byte) 34);
        vector.add((byte) 55);
        vector.add((byte) 56);
        vector.add((byte) -34);
        vector.add((byte) 67);
        vector.add((byte) 23);
        vector.add((byte) 34);
        vector.addNull();
        vector.add((byte) 6);
        vector.add((byte) 7);
        vector.add((byte) 65);
        vector.add((byte) 34);
        vector.add((byte) 78);
        return vector;
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
        ByteColumnWriter columnWriter = new ByteColumnWriter(
                TypeDescription.createByte(), writerOption);
        ByteColumnVector originVector = createSampleVector(numRows);
        columnWriter.write(originVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(expectedEncoding, encoding.getKind());
        System.out.println("--- selected round-trip encodingLevel=" + encodingLevel
                + ", nullsPadding=" + nullsPadding
                + ", encoding=" + encoding.getKind()
                + ", chunkBytes=" + content.length
                + ", vectorIndex=" + vectorIndex + " ---");
        printVector("origin  ", originVector, numRows);

        Bitmap selected = new Bitmap(numRows, true);
        // Skip non-null values so the reader must still consume their encoded payload.
        selected.clear(0);
        selected.clear(2);
        selected.clear(5);
        selected.clear(10);
        selected.clear(14);
        selected.clear(20);
        System.out.print("selected rows: ");
        for (int i = 0; i < numRows; ++i)
        {
            if (selected.get(i))
            {
                System.out.print(i + " ");
            }
        }
        System.out.println();

        ByteColumnReader columnReader = new ByteColumnReader(TypeDescription.createByte());
        ByteColumnVector targetVector = new ByteColumnVector(vectorIndex + numRows);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, vectorIndex, targetVector, chunkIndex, selected);
        columnReader.close();

        int targetIndex = vectorIndex;
        StringBuilder expectedSel = new StringBuilder("expected selected [");
        StringBuilder actualSel = new StringBuilder("actual selected   [");
        boolean first = true;
        for (int i = 0; i < numRows; ++i)
        {
            if (selected.get(i))
            {
                if (!first)
                {
                    expectedSel.append(", ");
                    actualSel.append(", ");
                }
                first = false;
                expectedSel.append(formatCell(originVector, i));
                actualSel.append(formatCell(targetVector, targetIndex));
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
        System.out.println(expectedSel.append(']'));
        System.out.println(actualSel.append(']'));
        System.out.println("OK: selected rows match, written from vectorIndex=" + vectorIndex);
    }

    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        ByteColumnWriter columnWriter = new ByteColumnWriter(
                TypeDescription.createByte(), writerOption);
        ByteColumnVector originVector = createSampleVector(numRows);
        columnWriter.write(originVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        System.out.println("encoding=" + encoding.getKind() + ", nullsPadding=true, chunkBytes=" + content.length);
        ByteColumnReader columnReader = new ByteColumnReader(TypeDescription.createByte());
        ByteColumnVector targetVector = new ByteColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, targetVector, chunkIndex);
        columnReader.close();

        assertVectorsEqual("NONE + nullsPadding", originVector, targetVector, numRows);
    }

    @Test
    public void testWithoutNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        ByteColumnWriter columnWriter = new ByteColumnWriter(
                TypeDescription.createByte(), writerOption);
        ByteColumnVector originVector = createSampleVector(numRows);
        columnWriter.write(originVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        System.out.println("encoding=" + encoding.getKind() + ", nullsPadding=false, chunkBytes=" + content.length);
        ByteColumnReader columnReader = new ByteColumnReader(TypeDescription.createByte());
        ByteColumnVector targetVector = new ByteColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, targetVector, chunkIndex);
        columnReader.close();

        assertVectorsEqual("NONE without nullsPadding", originVector, targetVector, numRows);
    }

    @Test
    public void testRunLength() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL2).nullsPadding(false);
        ByteColumnWriter columnWriter = new ByteColumnWriter(
                TypeDescription.createByte(), writerOption);
        ByteColumnVector originVector = new ByteColumnVector(numRows);
        // include repeats and nulls so RLE is exercised
        originVector.add((byte) 5);
        originVector.add((byte) 5);
        originVector.add((byte) 5);
        originVector.add((byte) 5);
        originVector.addNull();
        originVector.add((byte) 5);
        originVector.add((byte) 5);
        originVector.add((byte) 7);
        originVector.addNull();
        originVector.add((byte) 7);
        originVector.add((byte) 1);
        originVector.add((byte) 2);
        originVector.add((byte) 3);
        originVector.add((byte) 9);
        originVector.add((byte) 9);
        originVector.add((byte) 9);
        originVector.addNull();
        originVector.add((byte) 9);
        originVector.add((byte) 9);
        originVector.add((byte) -128);
        originVector.add((byte) 127);
        originVector.add((byte) 0);
        columnWriter.write(originVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH, encoding.getKind());
        System.out.println("encoding=" + encoding.getKind() + ", EL2, chunkBytes=" + content.length);
        ByteColumnReader columnReader = new ByteColumnReader(TypeDescription.createByte());
        ByteColumnVector targetVector = new ByteColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, targetVector, chunkIndex);
        columnReader.close();

        assertVectorsEqual("RUNLENGTH", originVector, targetVector, numRows);
    }

    @Test
    public void testSelected() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        ByteColumnWriter columnWriter = new ByteColumnWriter(
                TypeDescription.createByte(), writerOption);
        ByteColumnVector originVector = createSampleVector(numRows);
        columnWriter.write(originVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        System.out.println("--- selected (skip every 10th row) encoding=" + encoding.getKind()
                + ", chunkBytes=" + content.length + " ---");
        printVector("origin  ", originVector, numRows);
        ByteColumnReader columnReader = new ByteColumnReader(TypeDescription.createByte());
        ByteColumnVector targetVector = new ByteColumnVector(numRows);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, targetVector, chunkIndex, selected);
        columnReader.close();

        StringBuilder expectedSel = new StringBuilder("expected selected [");
        StringBuilder actualSel = new StringBuilder("actual selected   [");
        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                if (j > 0)
                {
                    expectedSel.append(", ");
                    actualSel.append(", ");
                }
                expectedSel.append(formatCell(originVector, i));
                actualSel.append(formatCell(targetVector, j));
                assertEquals(originVector.noNulls, targetVector.noNulls);
                assertEquals("isNull mismatch at src=" + i + " dst=" + j,
                        originVector.isNull[i], targetVector.isNull[j]);
                if (originVector.noNulls || !originVector.isNull[i])
                {
                    assertEquals("value mismatch at src=" + i + " dst=" + j,
                            originVector.vector[i], targetVector.vector[j]);
                }
                j++;
            }
        }
        System.out.println(expectedSel.append(']'));
        System.out.println(actualSel.append(']'));
        System.out.println("OK: selected rows match");
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
        ByteColumnWriter columnWriter = new ByteColumnWriter(
                TypeDescription.createByte(), writerOption);

        ByteColumnVector originVector = new ByteColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add((byte) ((j / 200) % 4));
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
        System.out.println("--- large fragmented ---");
        System.out.println("encoding=" + encoding.getKind()
                + ", batches=" + numBatches
                + ", rowsPerBatch=" + numRows
                + ", totalRows=" + totalRows
                + ", chunkBytes=" + content.length);
        System.out.println("read ranges: [0,123), [123,579), [579," + totalRows + ")");
        ByteColumnReader columnReader = new ByteColumnReader(TypeDescription.createByte());
        ByteColumnVector targetVector = new ByteColumnVector(totalRows);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        columnReader.read(buffer, encoding, 0, 123,
                10000, 0, targetVector, chunkIndex);
        columnReader.read(buffer, encoding, 123, 456,
                10000, 123, targetVector, chunkIndex);
        columnReader.read(buffer, encoding, 123 + 456, totalRows - 123 - 456,
                10000, 123 + 456, targetVector, chunkIndex);
        columnReader.close();

        int mismatches = 0;
        for (int i = 0; i < totalRows; i++)
        {
            assertEquals("isNull mismatch at row " + i,
                    originVector.isNull[i % numRows], targetVector.isNull[i]);
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                if (originVector.vector[i % numRows] != targetVector.vector[i])
                {
                    mismatches++;
                }
                assertEquals("value mismatch at row " + i,
                        originVector.vector[i % numRows], targetVector.vector[i]);
            }
        }
        System.out.println("sample actual [0..15]: "
                + java.util.Arrays.toString(java.util.Arrays.copyOf(targetVector.vector, 16)));
        System.out.println("OK: all " + totalRows + " rows match, mismatches=" + mismatches);
    }
}
