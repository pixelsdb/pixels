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
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.StringColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-21
 */
public class TestStringColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        StringColumnWriter columnWriter = new StringColumnWriter(
                TypeDescription.createString(), writerOption);
        BinaryColumnVector binaryColumnVector = new BinaryColumnVector(numRows);
        binaryColumnVector.add("100");
        binaryColumnVector.add("103");
        binaryColumnVector.add("106");
        binaryColumnVector.add("34");
        binaryColumnVector.addNull();
        binaryColumnVector.add("54");
        binaryColumnVector.add("55");
        binaryColumnVector.add("67");
        binaryColumnVector.addNull();
        binaryColumnVector.add("34");
        binaryColumnVector.add("555");
        binaryColumnVector.add("565");
        binaryColumnVector.add("234");
        binaryColumnVector.add("675");
        binaryColumnVector.add("235");
        binaryColumnVector.add("32434");
        binaryColumnVector.addNull();
        binaryColumnVector.add("6");
        binaryColumnVector.add("7");
        binaryColumnVector.add("65656565");
        binaryColumnVector.add("3434");
        binaryColumnVector.add("54578");
        columnWriter.write(binaryColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        StringColumnReader columnReader = new StringColumnReader(TypeDescription.createString());
        BinaryColumnVector binaryColumnVector1 = new BinaryColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, binaryColumnVector1, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert binaryColumnVector1.noNulls == binaryColumnVector.noNulls;
            assert binaryColumnVector1.isNull[i] == binaryColumnVector.isNull[i];
            if (binaryColumnVector.noNulls || !binaryColumnVector.isNull[i])
            {
                String s1 = new String(binaryColumnVector1.vector[i], binaryColumnVector1.start[i], binaryColumnVector1.lens[i]);
                String s = new String(binaryColumnVector.vector[i], binaryColumnVector.start[i], binaryColumnVector.lens[i]);
                assert s1.equals(s);
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
        StringColumnWriter columnWriter = new StringColumnWriter(
                TypeDescription.createString(), writerOption);
        BinaryColumnVector binaryColumnVector = new BinaryColumnVector(numRows);
        binaryColumnVector.add("100");
        binaryColumnVector.add("103");
        binaryColumnVector.add("106");
        binaryColumnVector.add("34");
        binaryColumnVector.addNull();
        binaryColumnVector.add("54");
        binaryColumnVector.add("55");
        binaryColumnVector.add("67");
        binaryColumnVector.addNull();
        binaryColumnVector.add("34");
        binaryColumnVector.add("555");
        binaryColumnVector.add("565");
        binaryColumnVector.add("234");
        binaryColumnVector.add("675");
        binaryColumnVector.add("235");
        binaryColumnVector.add("32434");
        binaryColumnVector.addNull();
        binaryColumnVector.add("6");
        binaryColumnVector.add("7");
        binaryColumnVector.add("65656565");
        binaryColumnVector.add("3434");
        binaryColumnVector.add("54578");
        columnWriter.write(binaryColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        StringColumnReader columnReader = new StringColumnReader(TypeDescription.createString());
        BinaryColumnVector binaryColumnVector1 = new BinaryColumnVector(numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, binaryColumnVector1, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numRows; ++i)
        {
            assert binaryColumnVector1.noNulls == binaryColumnVector.noNulls;
            assert binaryColumnVector1.isNull[i] == binaryColumnVector.isNull[i];
            if (binaryColumnVector.noNulls || !binaryColumnVector.isNull[i])
            {
                String s1 = new String(binaryColumnVector1.vector[i], binaryColumnVector1.start[i], binaryColumnVector1.lens[i]);
                String s = new String(binaryColumnVector.vector[i], binaryColumnVector.start[i], binaryColumnVector.lens[i]);
                assert s1.equals(s);
            }
        }
    }

    @Test
    public void testSelect() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        StringColumnWriter columnWriter = new StringColumnWriter(
                TypeDescription.createString(), writerOption);
        BinaryColumnVector binaryColumnVector = new BinaryColumnVector(numRows);
        binaryColumnVector.add("100");
        binaryColumnVector.add("103");
        binaryColumnVector.add("106");
        binaryColumnVector.add("34");
        binaryColumnVector.addNull();
        binaryColumnVector.add("54");
        binaryColumnVector.add("55");
        binaryColumnVector.add("67");
        binaryColumnVector.addNull();
        binaryColumnVector.add("34");
        binaryColumnVector.add("555");
        binaryColumnVector.add("565");
        binaryColumnVector.add("234");
        binaryColumnVector.add("675");
        binaryColumnVector.add("235");
        binaryColumnVector.add("32434");
        binaryColumnVector.addNull();
        binaryColumnVector.add("6");
        binaryColumnVector.add("7");
        binaryColumnVector.add("65656565");
        binaryColumnVector.add("3434");
        binaryColumnVector.add("54578");
        columnWriter.write(binaryColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        StringColumnReader columnReader = new StringColumnReader(TypeDescription.createString());
        BinaryColumnVector binaryColumnVector1 = new BinaryColumnVector(numRows);
        Bitmap selected = new Bitmap(numRows, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, binaryColumnVector1, chunkIndex, selected);
        columnReader.close();

        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assert binaryColumnVector1.noNulls == binaryColumnVector.noNulls;
                assert binaryColumnVector1.isNull[j] == binaryColumnVector.isNull[i];
                if (binaryColumnVector.noNulls || !binaryColumnVector.isNull[i])
                {
                    String s1 = new String(binaryColumnVector1.vector[j], binaryColumnVector1.start[j], binaryColumnVector1.lens[j]);
                    String s = new String(binaryColumnVector.vector[i], binaryColumnVector.start[i], binaryColumnVector.lens[i]);
                    assert s1.equals(s);
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
        StringColumnWriter columnWriter = new StringColumnWriter(
                TypeDescription.createString(), writerOption);

        BinaryColumnVector originVector = new BinaryColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            } else
            {
                originVector.add("1000");
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
        StringColumnReader columnReader = new StringColumnReader(TypeDescription.createString());
        BinaryColumnVector targetVector = new BinaryColumnVector(numBatches*numRows);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numBatches*numRows,
                10000, 0, targetVector, chunkIndex);
        columnReader.close();

        for (int i = 0; i < numBatches*numRows; i++)
        {
            int j = i % numRows;
            assert targetVector.isNull[i] == originVector.isNull[j];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                String s1 = new String(targetVector.vector[i], targetVector.start[i], targetVector.lens[i]);

                String s = new String(originVector.vector[j], originVector.start[j], originVector.lens[j]);
                assert s1.equals(s);
            }
        }
    }

    @Test
    public void testLargeFragment() throws IOException
    {
        int numBatches = 15;
        int numRows = 1024;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10000).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        StringColumnWriter columnWriter = new StringColumnWriter(
                TypeDescription.createString(), writerOption);

        BinaryColumnVector originVector = new BinaryColumnVector(numRows);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            } else
            {
                originVector.add("1000");
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
        StringColumnReader columnReader = new StringColumnReader(TypeDescription.createString());
        BinaryColumnVector targetVector = new BinaryColumnVector(numBatches*numRows);
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
            int j = i % numRows;
            assert targetVector.isNull[i] == originVector.isNull[j];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                String s1 = new String(targetVector.vector[i], targetVector.start[i], targetVector.lens[i]);

                String s = new String(originVector.vector[j], originVector.start[j], originVector.lens[j]);
                assert s1.equals(s);
            }
        }
    }
}
