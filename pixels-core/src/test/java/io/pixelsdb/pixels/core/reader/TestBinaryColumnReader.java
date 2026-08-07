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
import io.pixelsdb.pixels.core.writer.BinaryColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Memory round-trip tests for BINARY.
 * <p>
 * Sample values are slices with non-zero starts so the round trip verifies
 * {@link BinaryColumnVector#start} / {@link BinaryColumnVector#lens}.
 * On-disk layout is a 4-byte endian-aware length prefix plus payload; nulls
 * write no content bytes and {@link BinaryColumnWriter} always disables nulls padding.
 *
 * @author gengdy
 * @create 2026-08-07
 */
public class TestBinaryColumnReader
{
    private static final int BINARY_MAX_LENGTH = 32;
    private static final int PIXEL_STRIDE = 10;
    private static final int NUM_ROWS = 22;

    private static TypeDescription binaryType()
    {
        return TypeDescription.createBinary(BINARY_MAX_LENGTH);
    }

    private static TypeDescription binaryType(int maxLength)
    {
        return TypeDescription.createBinary(maxLength);
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
        addBytes(vector, new byte[] {'a', 'b'});
        addBytes(vector, new byte[] {9});
        vector.addNull();
        addBytes(vector, new byte[] {1, 1, 1, 1});
        addBytes(vector, new byte[] {5, 5, 5});
        addBytes(vector, new byte[] {6, 5, 6});
        addBytes(vector, new byte[] {2, 3, 4});
        addBytes(vector, new byte[] {6, 7, 5});
        addBytes(vector, new byte[] {2, 3, 5});
        addBytes(vector, new byte[BINARY_MAX_LENGTH]);
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

    private static void assertVectorsEqual(BinaryColumnVector expected, BinaryColumnVector actual,
                                           int actualOffset, int numRows)
    {
        for (int i = 0; i < numRows; ++i)
        {
            assertEquals("isNull mismatch at row " + i,
                    expected.isNull[i], actual.isNull[actualOffset + i]);
            if (expected.noNulls || !expected.isNull[i])
            {
                assertArrayEquals("value mismatch at row " + i,
                        valueAt(expected, i), valueAt(actual, actualOffset + i));
            }
        }
    }

    private static class Chunk
    {
        final byte[] content;
        final PixelsProto.ColumnChunkIndex chunkIndex;
        final PixelsProto.ColumnEncoding encoding;
        final BinaryColumnWriter writer;

        Chunk(byte[] content, PixelsProto.ColumnChunkIndex chunkIndex,
              PixelsProto.ColumnEncoding encoding, BinaryColumnWriter writer)
        {
            this.content = content;
            this.chunkIndex = chunkIndex;
            this.encoding = encoding;
            this.writer = writer;
        }
    }

    private static Chunk writeChunk(BinaryColumnVector source, int numRows, TypeDescription type,
                                    int pixelStride, ByteOrder byteOrder, boolean nullsPadding)
            throws IOException
    {
        BinaryColumnWriter writer = new BinaryColumnWriter(type,
                new PixelsWriterOption()
                        .pixelStride(pixelStride)
                        .byteOrder(byteOrder)
                        .encodingLevel(EncodingLevel.EL0)
                        .nullsPadding(nullsPadding));
        writer.write(source, numRows);
        writer.flush();
        return new Chunk(writer.getColumnChunkContent(),
                writer.getColumnChunkIndex().build(),
                writer.getColumnChunkEncoding().build(),
                writer);
    }

    private static ByteBuffer toDirectBufferWithOffset(byte[] content)
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(content.length + 1);
        buffer.put((byte) 0xAB);
        buffer.put(content);
        buffer.flip();
        buffer.position(1);
        return buffer;
    }

    private static void assertSelectedRoundTrip(ByteOrder byteOrder) throws IOException
    {
        int vectorIndex = 3;
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        Chunk chunk = writeChunk(origin, NUM_ROWS, binaryType(), PIXEL_STRIDE, byteOrder, false);
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, chunk.encoding.getKind());
        assertFalse(chunk.chunkIndex.getNullsPadding());

        Bitmap selected = new Bitmap(NUM_ROWS, true);
        selected.clear(0);
        selected.clear(2);
        selected.clear(4);
        selected.clear(5);
        selected.clear(10);
        selected.clear(14);
        selected.clear(16);
        selected.clear(20);

        BinaryColumnReader reader = new BinaryColumnReader(binaryType());
        BinaryColumnVector target = new BinaryColumnVector(vectorIndex + NUM_ROWS);
        reader.readSelected(ByteBuffer.wrap(chunk.content), chunk.encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, vectorIndex, target, chunk.chunkIndex, selected);
        reader.close();
        chunk.writer.close();

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
        // Writer always disables nulls padding; requesting true must still round-trip and record false.
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        Chunk chunk = writeChunk(origin, NUM_ROWS, binaryType(), PIXEL_STRIDE,
                ByteOrder.LITTLE_ENDIAN, true);
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, chunk.encoding.getKind());
        assertFalse(chunk.chunkIndex.getNullsPadding());
        assertArrayEquals(new byte[] {0, 0, 0, 0},
                Arrays.copyOfRange(chunk.content, 0, Integer.BYTES));

        BinaryColumnReader reader = new BinaryColumnReader(binaryType());
        BinaryColumnVector target = new BinaryColumnVector(NUM_ROWS);
        reader.read(ByteBuffer.wrap(chunk.content), chunk.encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, 0, target, chunk.chunkIndex);
        reader.close();
        chunk.writer.close();

        assertVectorsEqual(origin, target, 0, NUM_ROWS);
    }

    @Test
    public void testWithoutNullsPadding() throws IOException
    {
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        Chunk chunk = writeChunk(origin, NUM_ROWS, binaryType(), PIXEL_STRIDE,
                ByteOrder.LITTLE_ENDIAN, false);
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, chunk.encoding.getKind());
        assertFalse(chunk.chunkIndex.getNullsPadding());

        BinaryColumnReader reader = new BinaryColumnReader(binaryType());
        BinaryColumnVector target = new BinaryColumnVector(NUM_ROWS);
        reader.read(ByteBuffer.wrap(chunk.content), chunk.encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, 0, target, chunk.chunkIndex);
        reader.close();
        chunk.writer.close();

        assertVectorsEqual(origin, target, 0, NUM_ROWS);
    }

    @Test
    public void testBigEndianDirectBuffer() throws IOException
    {
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        Chunk chunk = writeChunk(origin, NUM_ROWS, binaryType(), PIXEL_STRIDE,
                ByteOrder.BIG_ENDIAN, false);
        assertFalse(chunk.chunkIndex.getLittleEndian());
        assertArrayEquals(new byte[] {0, 0, 0, 0},
                Arrays.copyOfRange(chunk.content, 0, Integer.BYTES));

        BinaryColumnReader reader = new BinaryColumnReader(binaryType());
        BinaryColumnVector target = new BinaryColumnVector(NUM_ROWS);
        reader.read(toDirectBufferWithOffset(chunk.content), chunk.encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, 0, target, chunk.chunkIndex);
        reader.close();
        chunk.writer.close();

        assertVectorsEqual(origin, target, 0, NUM_ROWS);
    }

    @Test
    public void testSelected() throws IOException
    {
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        Chunk chunk = writeChunk(origin, NUM_ROWS, binaryType(), PIXEL_STRIDE,
                ByteOrder.LITTLE_ENDIAN, false);

        Bitmap selected = new Bitmap(NUM_ROWS, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);

        BinaryColumnReader reader = new BinaryColumnReader(binaryType());
        BinaryColumnVector target = new BinaryColumnVector(NUM_ROWS);
        reader.readSelected(ByteBuffer.wrap(chunk.content), chunk.encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, 0, target, chunk.chunkIndex, selected);
        reader.close();
        chunk.writer.close();

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
        assertSelectedRoundTrip(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    public void testSelectedAtNonZeroVectorIndexBigEndianDirect() throws IOException
    {
        int vectorIndex = 3;
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        Chunk chunk = writeChunk(origin, NUM_ROWS, binaryType(), PIXEL_STRIDE,
                ByteOrder.BIG_ENDIAN, false);

        Bitmap selected = new Bitmap(NUM_ROWS, false);
        int[] selectedRows = new int[] {0, 1, 3, 5, 6, 8, 15, 21};
        for (int row : selectedRows)
        {
            selected.set(row);
        }

        BinaryColumnReader reader = new BinaryColumnReader(binaryType());
        BinaryColumnVector target = new BinaryColumnVector(vectorIndex + NUM_ROWS);
        reader.readSelected(toDirectBufferWithOffset(chunk.content), chunk.encoding, 0, NUM_ROWS,
                PIXEL_STRIDE, vectorIndex, target, chunk.chunkIndex, selected);
        reader.close();
        chunk.writer.close();

        int targetIndex = vectorIndex;
        for (int i = 0; i < NUM_ROWS; ++i)
        {
            if (!selected.get(i))
            {
                continue;
            }
            assertEquals(origin.isNull[i], target.isNull[targetIndex]);
            if (!origin.isNull[i])
            {
                assertArrayEquals(valueAt(origin, i), valueAt(target, targetIndex));
            }
            targetIndex++;
        }
    }

    @Test
    public void testTruncationAndVectorSlice() throws IOException
    {
        int maxLength = 4;
        TypeDescription type = binaryType(maxLength);
        BinaryColumnWriter writer = new BinaryColumnWriter(type,
                new PixelsWriterOption()
                        .pixelStride(PIXEL_STRIDE)
                        .byteOrder(ByteOrder.BIG_ENDIAN)
                        .encodingLevel(EncodingLevel.EL0)
                        .nullsPadding(false));

        BinaryColumnVector source = new BinaryColumnVector(4);
        byte[] first = new byte[] {99, 1, 2, 3, 4, 98};
        byte[] second = new byte[] {97, 96, 5, 6, 7, 8, 9, 10, 95};
        source.setRef(0, first, 1, 4);
        source.setRef(1, second, 2, 6);
        source.setRef(2, new byte[] {42}, 1, 0);
        source.addNull();

        writer.write(source, 4);
        writer.flush();
        byte[] content = writer.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = writer.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = writer.getColumnChunkEncoding().build();

        assertEquals(1, writer.getNumTruncated());
        assertEquals(8, writer.getColumnChunkStat().getBinaryStatistics().getSum());
        assertArrayEquals(new byte[] {0, 0, 0, 4, 1, 2, 3, 4},
                Arrays.copyOfRange(content, 0, 8));

        BinaryColumnReader reader = new BinaryColumnReader(type);
        BinaryColumnVector target = new BinaryColumnVector(4);
        reader.read(ByteBuffer.wrap(content), encoding, 0, 4, PIXEL_STRIDE, 0, target, chunkIndex);
        reader.close();

        BinaryColumnVector expected = new BinaryColumnVector(4);
        expected.add(new byte[] {1, 2, 3, 4});
        expected.add(new byte[] {5, 6, 7, 8});
        expected.add(new byte[0]);
        expected.addNull();
        assertVectorsEqual(expected, target, 0, 4);

        writer.reset();
        assertEquals(0, writer.getNumTruncated());
        writer.close();
    }

    @Test
    public void testFragmentedRead() throws IOException
    {
        int pixelStride = 4;
        BinaryColumnVector origin = createSampleVector(NUM_ROWS);
        Chunk chunk = writeChunk(origin, NUM_ROWS, binaryType(), pixelStride,
                ByteOrder.LITTLE_ENDIAN, false);

        int vectorIndex = 2;
        BinaryColumnReader reader = new BinaryColumnReader(binaryType());
        BinaryColumnVector target = new BinaryColumnVector(vectorIndex + NUM_ROWS);
        ByteBuffer buffer = ByteBuffer.wrap(chunk.content);
        reader.read(buffer, chunk.encoding, 0, 3, pixelStride, vectorIndex, target, chunk.chunkIndex);
        reader.read(buffer, chunk.encoding, 3, 7, pixelStride, vectorIndex + 3, target, chunk.chunkIndex);
        reader.read(buffer, chunk.encoding, 10, NUM_ROWS - 10, pixelStride,
                vectorIndex + 10, target, chunk.chunkIndex);
        reader.close();
        chunk.writer.close();

        assertVectorsEqual(origin, target, vectorIndex, NUM_ROWS);
    }

    @Test
    public void testLargeFragmented() throws IOException
    {
        int numBatches = 15;
        int numRows = 1024;
        int pixelStride = 10000;
        BinaryColumnWriter writer = new BinaryColumnWriter(binaryType(),
                new PixelsWriterOption()
                        .pixelStride(pixelStride)
                        .byteOrder(ByteOrder.LITTLE_ENDIAN)
                        .encodingLevel(EncodingLevel.EL0)
                        .nullsPadding(false));

        BinaryColumnVector origin = new BinaryColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                origin.addNull();
            }
            else
            {
                addBytes(origin, new byte[] {(byte) (j & 0xFF), (byte) ((j >> 8) & 0xFF), 3, 4});
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
        BinaryColumnReader reader = new BinaryColumnReader(binaryType());
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
