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
 * Round-trip tests for {@link BinaryColumnReader} / {@link BinaryColumnWriter}.
 * {@link VarbinaryColumnReader} is covered by a thin smoke test because it only
 * subclasses the binary reader/writer.
 */
public class TestBinaryColumnReader
{
    private static final int PIXEL_STRIDE = 4;

    private static String formatCell(BinaryColumnVector vector, int i)
    {
        if (!vector.noNulls && vector.isNull[i])
        {
            return "NULL";
        }
        byte[] bytes = Arrays.copyOfRange(vector.vector[i],
                vector.start[i], vector.start[i] + vector.lens[i]);
        return Arrays.toString(bytes);
    }

    private static void printVector(String label, BinaryColumnVector vector, int from, int numRows)
    {
        StringBuilder sb = new StringBuilder(label).append(" [");
        for (int i = 0; i < numRows; ++i)
        {
            if (i > 0)
            {
                sb.append(", ");
            }
            sb.append(formatCell(vector, from + i));
        }
        sb.append(']');
        System.out.println(sb);
    }

    private static void assertVectorsEqual(String caseName, BinaryColumnVector expected,
                                           BinaryColumnVector actual, int actualOffset, int numRows)
    {
        System.out.println("--- " + caseName + " ---");
        printVector("expected", expected, 0, numRows);
        printVector("actual  ", actual, actualOffset, numRows);
        for (int i = 0; i < numRows; ++i)
        {
            assertEquals("isNull mismatch at row " + i,
                    expected.isNull[i], actual.isNull[actualOffset + i]);
            if (!expected.isNull[i])
            {
                assertArrayEquals("value mismatch at row " + i,
                        Arrays.copyOfRange(expected.vector[i],
                                expected.start[i], expected.start[i] + expected.lens[i]),
                        Arrays.copyOfRange(actual.vector[actualOffset + i],
                                actual.start[actualOffset + i],
                                actual.start[actualOffset + i] + actual.lens[actualOffset + i]));
            }
        }
        System.out.println("OK: all " + numRows + " rows match");
    }

    private static BinaryColumnVector createSampleVector()
    {
        // Mix empty, null, short and longer payloads across multiple pixels (stride=4).
        BinaryColumnVector vector = new BinaryColumnVector(9);
        vector.add(new byte[] {1, 2, 3});
        vector.addNull();
        vector.add(new byte[0]);
        vector.add(new byte[] {(byte) 0xFF, 0x00});
        vector.add(new byte[] {10, 11, 12, 13, 14});
        vector.add(new byte[] {42});
        vector.addNull();
        vector.add(new byte[] {7, 8});
        vector.add(new byte[] {9});
        return vector;
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

    private static Chunk writeChunk(BinaryColumnVector source, int numRows,
                                    TypeDescription type, ByteOrder byteOrder) throws IOException
    {
        BinaryColumnWriter writer = new BinaryColumnWriter(type,
                new PixelsWriterOption()
                        .pixelStride(PIXEL_STRIDE)
                        .byteOrder(byteOrder)
                        .encodingLevel(EncodingLevel.EL0)
                        .nullsPadding(true));
        writer.write(source, numRows);
        writer.flush();
        return new Chunk(writer.getColumnChunkContent(),
                writer.getColumnChunkIndex().build(),
                writer.getColumnChunkEncoding().build(),
                writer);
    }

    private static ByteBuffer toHeapBuffer(byte[] content)
    {
        return ByteBuffer.wrap(content);
    }

    private static ByteBuffer toDirectBufferWithOffset(byte[] content)
    {
        // Pad one leading byte so position != 0, covering the direct-buffer copy path.
        ByteBuffer buffer = ByteBuffer.allocateDirect(content.length + 1);
        buffer.put((byte) 0xAB);
        buffer.put(content);
        buffer.flip();
        buffer.position(1);
        return buffer;
    }

    @Test
    public void testRoundTripLittleEndian() throws IOException
    {
        TypeDescription type = TypeDescription.createBinary(64);
        BinaryColumnVector origin = createSampleVector();
        int numRows = origin.getLength();
        Chunk chunk = writeChunk(origin, numRows, type, ByteOrder.LITTLE_ENDIAN);

        System.out.println("encoding=" + chunk.encoding.getKind()
                + ", littleEndian=" + chunk.chunkIndex.getLittleEndian()
                + ", nullsPadding=" + chunk.chunkIndex.getNullsPadding()
                + ", isNullOffset=" + chunk.chunkIndex.getIsNullOffset()
                + ", chunkBytes=" + chunk.content.length
                + ", pixelStride=" + PIXEL_STRIDE);
        assertEquals(PixelsProto.ColumnEncoding.Kind.NONE, chunk.encoding.getKind());
        assertFalse(chunk.chunkIndex.getNullsPadding());
        // First value length prefix: 3 in little endian.
        assertArrayEquals(new byte[] {3, 0, 0, 0},
                Arrays.copyOfRange(chunk.content, 0, Integer.BYTES));

        BinaryColumnReader reader = new BinaryColumnReader(type);
        BinaryColumnVector target = new BinaryColumnVector(numRows);
        reader.read(toHeapBuffer(chunk.content), chunk.encoding, 0, numRows,
                PIXEL_STRIDE, 0, target, chunk.chunkIndex);
        reader.close();
        chunk.writer.close();

        assertVectorsEqual("BINARY little-endian heap round-trip", origin, target, 0, numRows);
    }

    @Test
    public void testBigEndianDirectBuffer() throws IOException
    {
        TypeDescription type = TypeDescription.createBinary(64);
        BinaryColumnVector origin = createSampleVector();
        int numRows = origin.getLength();
        Chunk chunk = writeChunk(origin, numRows, type, ByteOrder.BIG_ENDIAN);

        System.out.println("encoding=" + chunk.encoding.getKind()
                + ", littleEndian=" + chunk.chunkIndex.getLittleEndian()
                + ", chunkBytes=" + chunk.content.length
                + ", buffer=direct+offset");
        assertArrayEquals(new byte[] {0, 0, 0, 3},
                Arrays.copyOfRange(chunk.content, 0, Integer.BYTES));

        BinaryColumnReader reader = new BinaryColumnReader(type);
        BinaryColumnVector target = new BinaryColumnVector(numRows);
        ByteBuffer input = toDirectBufferWithOffset(chunk.content);
        System.out.println("input.hasArray=" + input.hasArray()
                + ", input.position=" + input.position()
                + ", input.remaining=" + input.remaining());
        reader.read(input, chunk.encoding, 0, numRows,
                PIXEL_STRIDE, 0, target, chunk.chunkIndex);
        reader.close();
        chunk.writer.close();

        assertVectorsEqual("BINARY big-endian direct-buffer round-trip", origin, target, 0, numRows);
    }

    @Test
    public void testTruncationAndVectorSlice() throws IOException
    {
        TypeDescription type = TypeDescription.createBinary(4);
        BinaryColumnWriter writer = new BinaryColumnWriter(type,
                new PixelsWriterOption()
                        .pixelStride(PIXEL_STRIDE)
                        .byteOrder(ByteOrder.BIG_ENDIAN)
                        .encodingLevel(EncodingLevel.EL0)
                        .nullsPadding(false));

        BinaryColumnVector source = new BinaryColumnVector(4);
        // Values are slices into larger arrays; second value is truncated to maxLength=4.
        byte[] first = new byte[] {99, 1, 2, 3, 4, 98};
        byte[] second = new byte[] {97, 96, 5, 6, 7, 8, 9, 10, 95};
        source.setRef(0, first, 1, 4);
        source.setRef(1, second, 2, 6);
        source.setRef(2, new byte[] {42}, 1, 0);
        source.addNull();

        System.out.println("write maxLength=4, source lens="
                + Arrays.toString(Arrays.copyOf(source.lens, 4)));
        printVector("source before write", source, 0, 4);

        writer.write(source, 4);
        writer.flush();
        byte[] content = writer.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = writer.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = writer.getColumnChunkEncoding().build();

        System.out.println("numTruncated=" + writer.getNumTruncated()
                + ", binarySum=" + writer.getColumnChunkStat().getBinaryStatistics().getSum()
                + ", chunkBytes=" + content.length);
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
        assertVectorsEqual("truncation + setRef slices", expected, target, 0, 4);

        writer.reset();
        System.out.println("after reset, numTruncated=" + writer.getNumTruncated());
        assertEquals(0, writer.getNumTruncated());
        writer.close();
    }

    @Test
    public void testFragmentedRead() throws IOException
    {
        TypeDescription type = TypeDescription.createBinary(32);
        BinaryColumnVector origin = createSampleVector();
        int numRows = origin.getLength();
        Chunk chunk = writeChunk(origin, numRows, type, ByteOrder.LITTLE_ENDIAN);

        int vectorIndex = 2;
        BinaryColumnReader reader = new BinaryColumnReader(type);
        BinaryColumnVector target = new BinaryColumnVector(vectorIndex + numRows);
        ByteBuffer buffer = ByteBuffer.wrap(chunk.content);

        System.out.println("fragmented read: batches [0,3), [3,7), [7,9) into vectorIndex=" + vectorIndex);
        reader.read(buffer, chunk.encoding, 0, 3, PIXEL_STRIDE, vectorIndex, target, chunk.chunkIndex);
        System.out.println("after batch1, element range written=[" + vectorIndex + ", " + (vectorIndex + 3) + ")");
        reader.read(buffer, chunk.encoding, 3, 4, PIXEL_STRIDE, vectorIndex + 3, target, chunk.chunkIndex);
        System.out.println("after batch2, element range written=[" + (vectorIndex + 3) + ", " + (vectorIndex + 7) + ")");
        reader.read(buffer, chunk.encoding, 7, 2, PIXEL_STRIDE, vectorIndex + 7, target, chunk.chunkIndex);
        System.out.println("after batch3, element range written=[" + (vectorIndex + 7) + ", " + (vectorIndex + 9) + ")");
        reader.close();
        chunk.writer.close();

        assertVectorsEqual("fragmented read across pixels", origin, target, vectorIndex, numRows);
    }

    @Test
    public void testReadSelected() throws IOException
    {
        TypeDescription type = TypeDescription.createBinary(32);
        BinaryColumnVector origin = createSampleVector();
        int numRows = origin.getLength();
        Chunk chunk = writeChunk(origin, numRows, type, ByteOrder.LITTLE_ENDIAN);

        int vectorIndex = 3;
        Bitmap selected = new Bitmap(numRows, false);
        // Include both null and non-null rows; skipped non-nulls must still advance the content cursor.
        int[] selectedRows = new int[] {0, 1, 3, 5, 6, 8};
        for (int row : selectedRows)
        {
            selected.set(row);
        }

        System.out.println("readSelected: vectorIndex=" + vectorIndex
                + ", chunkBytes=" + chunk.content.length
                + ", buffer=direct+offset");
        System.out.print("selected rows: ");
        for (int row : selectedRows)
        {
            System.out.print(row + " ");
        }
        System.out.println();
        printVector("origin  ", origin, 0, numRows);

        BinaryColumnReader reader = new BinaryColumnReader(type);
        BinaryColumnVector target = new BinaryColumnVector(vectorIndex + numRows);
        reader.readSelected(toDirectBufferWithOffset(chunk.content), chunk.encoding, 0, numRows,
                PIXEL_STRIDE, vectorIndex, target, chunk.chunkIndex, selected);
        reader.close();
        chunk.writer.close();

        int targetIndex = vectorIndex;
        StringBuilder expectedSel = new StringBuilder("expected selected [");
        StringBuilder actualSel = new StringBuilder("actual selected   [");
        boolean first = true;
        for (int i = 0; i < numRows; ++i)
        {
            if (!selected.get(i))
            {
                continue;
            }
            if (!first)
            {
                expectedSel.append(", ");
                actualSel.append(", ");
            }
            first = false;
            expectedSel.append(formatCell(origin, i));
            actualSel.append(formatCell(target, targetIndex));
            assertEquals("isNull mismatch at selected src=" + i + " dst=" + targetIndex,
                    origin.isNull[i], target.isNull[targetIndex]);
            if (!origin.isNull[i])
            {
                assertArrayEquals("value mismatch at selected src=" + i + " dst=" + targetIndex,
                        Arrays.copyOfRange(origin.vector[i],
                                origin.start[i], origin.start[i] + origin.lens[i]),
                        Arrays.copyOfRange(target.vector[targetIndex],
                                target.start[targetIndex],
                                target.start[targetIndex] + target.lens[targetIndex]));
            }
            targetIndex++;
        }
        System.out.println(expectedSel.append(']'));
        System.out.println(actualSel.append(']'));
        System.out.println("OK: selected rows match, written from vectorIndex=" + vectorIndex);
    }

    @Test
    public void testVarbinarySmoke() throws IOException
    {
        // Varbinary reader/writer are thin subclasses of binary; one smoke case is enough.
        TypeDescription type = TypeDescription.createVarbinary(32);
        BinaryColumnVector origin = new BinaryColumnVector(3);
        origin.add(new byte[] {1, 2});
        origin.addNull();
        origin.add(new byte[] {3});

        VarbinaryColumnWriter writer = new VarbinaryColumnWriter(type,
                new PixelsWriterOption()
                        .pixelStride(PIXEL_STRIDE)
                        .byteOrder(ByteOrder.LITTLE_ENDIAN)
                        .encodingLevel(EncodingLevel.EL0)
                        .nullsPadding(false));
        writer.write(origin, 3);
        writer.flush();
        byte[] content = writer.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = writer.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = writer.getColumnChunkEncoding().build();

        System.out.println("VARBINARY smoke: encoding=" + encoding.getKind()
                + ", chunkBytes=" + content.length);
        VarbinaryColumnReader reader = new VarbinaryColumnReader(type);
        BinaryColumnVector target = new BinaryColumnVector(3);
        reader.read(ByteBuffer.wrap(content), encoding, 0, 3, PIXEL_STRIDE, 0, target, chunkIndex);
        reader.close();
        writer.close();

        assertVectorsEqual("VARBINARY smoke round-trip", origin, target, 0, 3);
    }
}
