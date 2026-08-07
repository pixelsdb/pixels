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
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.VarbinaryColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Memory round-trip tests for VARBINARY.
 * <p>
 * VARBINARY shares the BINARY physical layout via
 * {@link VarbinaryColumnWriter} / {@link VarbinaryColumnReader}. Sample values
 * use non-zero {@link BinaryColumnVector#start} slices.
 *
 * @author gengdy
 * @create 2026-08-07
 */
public class TestVarbinaryColumnReader
{
    private static final int VARBINARY_MAX_LENGTH = 32;
    private static final int PIXEL_STRIDE = 10;
    private static final int NUM_ROWS = 22;

    private static TypeDescription varbinaryType()
    {
        return TypeDescription.createVarbinary(VARBINARY_MAX_LENGTH);
    }

    private static void addBytes(BinaryColumnVector vector, byte[] value)
    {
        byte[] backing = new byte[value.length + 2];
        backing[0] = (byte) 0xA5;
        System.arraycopy(value, 0, backing, 1, value.length);
        backing[backing.length - 1] = (byte) 0x5A;
        vector.setRef(vector.getWriteIndex(), backing, 1, value.length);
    }

    private static BinaryColumnVector createSampleVector(int numRows)
    {
        BinaryColumnVector vector = new BinaryColumnVector(numRows);
        addBytes(vector, new byte[] {});
        addBytes(vector, new byte[] {0});
        addBytes(vector, new byte[] {1, 2, 3});
        addBytes(vector, new byte[] {(byte) 0xFF, (byte) 0x00, (byte) 0x7F});
        vector.addNull();
        addBytes(vector, new byte[] {10, 20, 30, 40, 50});
        addBytes(vector, new byte[] {'v', 'a', 'r'});
        addBytes(vector, new byte[] {9});
        vector.addNull();
        addBytes(vector, new byte[] {1, 1, 1, 1});
        addBytes(vector, new byte[] {5, 5, 5});
        addBytes(vector, new byte[] {6, 5, 6});
        addBytes(vector, new byte[] {2, 3, 4});
        addBytes(vector, new byte[] {6, 7, 5});
        addBytes(vector, new byte[] {2, 3, 5});
        addBytes(vector, new byte[VARBINARY_MAX_LENGTH]);
        vector.addNull();
        addBytes(vector, new byte[] {6});
        addBytes(vector, new byte[] {7});
        addBytes(vector, new byte[] {(byte) 0x80, (byte) 0x81, (byte) 0xFE, (byte) 0xFF});
        addBytes(vector, new byte[] {34, 34});
        addBytes(vector, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        return vector;
    }

    private static byte[] valueAt(BinaryColumnVector vector, int i)
    {
        return Arrays.copyOfRange(vector.vector[i], vector.start[i], vector.start[i] + vector.lens[i]);
    }

    private static void assertVectorsEqual(BinaryColumnVector expected, BinaryColumnVector actual, int numRows)
    {
        for (int i = 0; i < numRows; ++i)
        {
            assertEquals("isNull mismatch at row " + i, expected.isNull[i], actual.isNull[i]);
            if (expected.noNulls || !expected.isNull[i])
            {
                assertArrayEquals("value mismatch at row " + i, valueAt(expected, i), valueAt(actual, i));
            }
        }
    }

    private static void assertSelectedRoundTrip() throws IOException
    {
        int vectorIndex = 3;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(PIXEL_STRIDE).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        VarbinaryColumnWriter writer = new VarbinaryColumnWriter(varbinaryType(), writerOption);
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        writer.write(origin, NUM_ROWS);
        writer.flush();
        writer.close();

        byte[] content = writer.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = writer.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = writer.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        assertFalse(chunkIndex.getNullsPadding());

        Bitmap selected = new Bitmap(NUM_ROWS, true);
        selected.clear(0);
        selected.clear(2);
        selected.clear(4);
        selected.clear(5);
        selected.clear(10);
        selected.clear(14);
        selected.clear(16);
        selected.clear(20);

        VarbinaryColumnReader reader = new VarbinaryColumnReader(varbinaryType());
        BinaryColumnVector target = new BinaryColumnVector(vectorIndex + NUM_ROWS);
        reader.readSelected(ByteBuffer.wrap(content), encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, vectorIndex, target, chunkIndex, selected);
        reader.close();

        int targetIndex = vectorIndex;
        for (int i = 0; i < NUM_ROWS; ++i)
        {
            if (selected.get(i))
            {
                assertEquals("isNull mismatch at selected src=" + i + " dst=" + targetIndex,
                        origin.isNull[i], target.isNull[targetIndex]);
                if (!origin.isNull[i])
                {
                    assertArrayEquals("value mismatch at selected src=" + i + " dst=" + targetIndex,
                            valueAt(origin, i), valueAt(target, targetIndex));
                }
                targetIndex++;
            }
        }
    }

    @Test
    public void testNullsPaddingOptionIgnored() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(PIXEL_STRIDE).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        VarbinaryColumnWriter writer = new VarbinaryColumnWriter(varbinaryType(), writerOption);
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        writer.write(origin, NUM_ROWS);
        writer.flush();
        writer.close();

        byte[] content = writer.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = writer.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = writer.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        assertFalse(chunkIndex.getNullsPadding());

        VarbinaryColumnReader reader = new VarbinaryColumnReader(varbinaryType());
        BinaryColumnVector target = new BinaryColumnVector(NUM_ROWS);
        reader.read(ByteBuffer.wrap(content), encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, 0, target, chunkIndex);
        reader.close();

        assertVectorsEqual(origin, target, NUM_ROWS);
    }

    @Test
    public void testWithoutNullsPadding() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(PIXEL_STRIDE).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        VarbinaryColumnWriter writer = new VarbinaryColumnWriter(varbinaryType(), writerOption);
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        writer.write(origin, NUM_ROWS);
        writer.flush();
        writer.close();

        byte[] content = writer.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = writer.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = writer.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());
        assertFalse(chunkIndex.getNullsPadding());

        VarbinaryColumnReader reader = new VarbinaryColumnReader(varbinaryType());
        BinaryColumnVector target = new BinaryColumnVector(NUM_ROWS);
        reader.read(ByteBuffer.wrap(content), encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, 0, target, chunkIndex);
        reader.close();

        assertVectorsEqual(origin, target, NUM_ROWS);
    }

    @Test
    public void testSelected() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(PIXEL_STRIDE).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        VarbinaryColumnWriter writer = new VarbinaryColumnWriter(varbinaryType(), writerOption);
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        writer.write(origin, NUM_ROWS);
        writer.flush();
        writer.close();

        byte[] content = writer.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = writer.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = writer.getColumnChunkEncoding().build();
        VarbinaryColumnReader reader = new VarbinaryColumnReader(varbinaryType());
        BinaryColumnVector target = new BinaryColumnVector(NUM_ROWS);
        Bitmap selected = new Bitmap(NUM_ROWS, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        reader.readSelected(ByteBuffer.wrap(content), encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, 0, target, chunkIndex, selected);
        reader.close();

        for (int i = 0, j = 0; i < NUM_ROWS; ++i)
        {
            if (i % 10 != 0)
            {
                assertEquals(origin.isNull[i], target.isNull[j]);
                if (origin.noNulls || !origin.isNull[i])
                {
                    assertArrayEquals(valueAt(origin, i), valueAt(target, j));
                }
                j++;
            }
        }
    }

    @Test
    public void testSelectedAtNonZeroVectorIndex() throws IOException
    {
        assertSelectedRoundTrip();
    }

    @Test
    public void testLargeFragmented() throws IOException
    {
        int numBatches = 15;
        int numRows = 1024;
        int pixelStride = 10000;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        VarbinaryColumnWriter writer = new VarbinaryColumnWriter(varbinaryType(), writerOption);

        BinaryColumnVector origin = new BinaryColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                origin.addNull();
            }
            else
            {
                addBytes(origin, new byte[] {(byte) (j & 0xFF), (byte) ((j >> 8) & 0xFF), 7, 8});
            }
        }

        for (int i = 0; i < numBatches; i++)
        {
            writer.write(origin, numRows);
        }
        writer.flush();
        writer.close();

        byte[] content = writer.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = writer.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = writer.getColumnChunkEncoding().build();
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, encoding.getKind());

        int totalRows = numBatches * numRows;
        VarbinaryColumnReader reader = new VarbinaryColumnReader(varbinaryType());
        BinaryColumnVector target = new BinaryColumnVector(totalRows);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        reader.read(buffer, encoding, 0, 123, pixelStride, 0, target, chunkIndex);
        reader.read(buffer, encoding, 123, 456, pixelStride, 123, target, chunkIndex);
        reader.read(buffer, encoding, 123 + 456, totalRows - 123 - 456,
                pixelStride, 123 + 456, target, chunkIndex);
        reader.close();

        for (int i = 0; i < totalRows; i++)
        {
            int j = i % numRows;
            assertEquals(origin.isNull[j], target.isNull[i]);
            if (target.noNulls || !target.isNull[i])
            {
                assertArrayEquals(valueAt(origin, j), valueAt(target, i));
            }
        }
    }
}
