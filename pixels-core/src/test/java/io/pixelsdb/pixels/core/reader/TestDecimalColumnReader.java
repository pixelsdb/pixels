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
import io.pixelsdb.pixels.core.vector.DecimalColumnVector;
import io.pixelsdb.pixels.core.writer.DecimalColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestDecimalColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        int pixelsStride = 10;
        int numRows = 22;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(pixelsStride).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        DecimalColumnWriter columnWriter = new DecimalColumnWriter(
                TypeDescription.createDecimal(15, 2), writerOption);
        DecimalColumnVector decimalColumnVector = new DecimalColumnVector(numRows, 15, 2);
        decimalColumnVector.add(100.22);
        decimalColumnVector.add(103.32);
        decimalColumnVector.add(106.43);
        decimalColumnVector.add(34.10);
        decimalColumnVector.addNull();
        decimalColumnVector.add(54.09);
        decimalColumnVector.add(55.00);
        decimalColumnVector.add(67.23);
        decimalColumnVector.addNull();
        decimalColumnVector.add(34.58);
        decimalColumnVector.add(555.98);
        decimalColumnVector.add(565.76);
        decimalColumnVector.add(234.11);
        decimalColumnVector.add(675.34);
        decimalColumnVector.add(235.58);
        decimalColumnVector.add(32434.68);
        decimalColumnVector.addNull();
        decimalColumnVector.add(6.66);
        decimalColumnVector.add(7.77);
        decimalColumnVector.add(65656565.20);
        decimalColumnVector.add(3434.11);
        decimalColumnVector.add(54578.22);
        columnWriter.write(decimalColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DecimalColumnReader columnReader = new DecimalColumnReader(TypeDescription.createDecimal(15, 2));
        DecimalColumnVector decimalColumnVector1 = new DecimalColumnVector(numRows, 15, 2);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, decimalColumnVector1, chunkIndex);
        for (int i = 0; i < numRows; ++i)
        {
            assert decimalColumnVector1.noNulls == decimalColumnVector.noNulls;
            assert decimalColumnVector1.isNull[i] == decimalColumnVector.isNull[i];
            if (decimalColumnVector.noNulls || !decimalColumnVector.isNull[i])
            {
                assert decimalColumnVector1.vector[i] == decimalColumnVector.vector[i];
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
        DecimalColumnWriter columnWriter = new DecimalColumnWriter(
                TypeDescription.createDecimal(15, 2), writerOption);
        DecimalColumnVector decimalColumnVector = new DecimalColumnVector(numRows, 15, 2);
        decimalColumnVector.add(100.22);
        decimalColumnVector.add(103.32);
        decimalColumnVector.add(106.43);
        decimalColumnVector.add(34.10);
        decimalColumnVector.addNull();
        decimalColumnVector.add(54.09);
        decimalColumnVector.add(55.00);
        decimalColumnVector.add(67.23);
        decimalColumnVector.addNull();
        decimalColumnVector.add(34.58);
        decimalColumnVector.add(555.98);
        decimalColumnVector.add(565.76);
        decimalColumnVector.add(234.11);
        decimalColumnVector.add(675.34);
        decimalColumnVector.add(235.58);
        decimalColumnVector.add(32434.68);
        decimalColumnVector.addNull();
        decimalColumnVector.add(6.66);
        decimalColumnVector.add(7.77);
        decimalColumnVector.add(65656565.20);
        decimalColumnVector.add(3434.11);
        decimalColumnVector.add(54578.22);
        columnWriter.write(decimalColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DecimalColumnReader columnReader = new DecimalColumnReader(TypeDescription.createDecimal(15, 2));
        DecimalColumnVector decimalColumnVector1 = new DecimalColumnVector(numRows, 15, 2);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numRows,
                pixelsStride, 0, decimalColumnVector1, chunkIndex);
        for (int i = 0; i < numRows; ++i)
        {
            assert decimalColumnVector1.noNulls == decimalColumnVector.noNulls;
            assert decimalColumnVector1.isNull[i] == decimalColumnVector.isNull[i];
            if (decimalColumnVector.noNulls || !decimalColumnVector.isNull[i])
            {
                assert decimalColumnVector1.vector[i] == decimalColumnVector.vector[i];
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
        DecimalColumnWriter columnWriter = new DecimalColumnWriter(
                TypeDescription.createDecimal(15, 2), writerOption);
        DecimalColumnVector decimalColumnVector = new DecimalColumnVector(numRows, 15, 2);
        decimalColumnVector.add(100.22);
        decimalColumnVector.add(103.32);
        decimalColumnVector.add(106.43);
        decimalColumnVector.add(34.10);
        decimalColumnVector.addNull();
        decimalColumnVector.add(54.09);
        decimalColumnVector.add(55.00);
        decimalColumnVector.add(67.23);
        decimalColumnVector.addNull();
        decimalColumnVector.add(34.58);
        decimalColumnVector.add(555.98);
        decimalColumnVector.add(565.76);
        decimalColumnVector.add(234.11);
        decimalColumnVector.add(675.34);
        decimalColumnVector.add(235.58);
        decimalColumnVector.add(32434.68);
        decimalColumnVector.addNull();
        decimalColumnVector.add(6.66);
        decimalColumnVector.add(7.77);
        decimalColumnVector.add(65656565.20);
        decimalColumnVector.add(3434.11);
        decimalColumnVector.add(54578.22);
        columnWriter.write(decimalColumnVector, numRows);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DecimalColumnReader columnReader = new DecimalColumnReader(TypeDescription.createDecimal(15, 2));
        DecimalColumnVector decimalColumnVector1 = new DecimalColumnVector(numRows, 15, 2);
        Bitmap selected = new Bitmap(22, true);
        selected.clear(0);
        selected.clear(10);
        selected.clear(20);
        columnReader.readSelected(ByteBuffer.wrap(content), encoding, 0, 22,
                pixelsStride, 0, decimalColumnVector1, chunkIndex, selected);
        for (int i = 0, j = 0; i < numRows; ++i)
        {
            if (i % 10 != 0)
            {
                assert decimalColumnVector1.noNulls == decimalColumnVector.noNulls;
                assert decimalColumnVector1.isNull[j] == decimalColumnVector.isNull[i];
                if (decimalColumnVector.noNulls || !decimalColumnVector.isNull[i])
                {
                    assert decimalColumnVector1.vector[j] == decimalColumnVector.vector[i];
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
        DecimalColumnWriter columnWriter = new DecimalColumnWriter(
                TypeDescription.createDecimal(15, 2), writerOption);

        DecimalColumnVector originVector = new DecimalColumnVector(numRows, 15, 2);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000.00d);
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
        DecimalColumnReader columnReader = new DecimalColumnReader(TypeDescription.createDecimal(15, 2));
        DecimalColumnVector targetVector = new DecimalColumnVector(numBatches*numRows, 15, 2);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, numBatches*numRows,
                10000, 0, targetVector, chunkIndex);

        for (int i = 0; i < numBatches*numRows; i++)
        {
            int j = i % numRows;
            assert targetVector.isNull[i] == originVector.isNull[j];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                assert originVector.vector[j] == targetVector.vector[i];
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
        DecimalColumnWriter columnWriter = new DecimalColumnWriter(
                TypeDescription.createDecimal(15, 2), writerOption);

        DecimalColumnVector originVector = new DecimalColumnVector(numRows, 15, 2);
        for (int j = 0; j < numRows; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000.00d);
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
        DecimalColumnReader columnReader = new DecimalColumnReader(TypeDescription.createDecimal(15, 2));
        DecimalColumnVector targetVector = new DecimalColumnVector(numBatches*numRows, 15, 2);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 123,
                10000, 0, targetVector, chunkIndex);
        columnReader.read(ByteBuffer.wrap(content), encoding, 123, 456,
                10000, 123, targetVector, chunkIndex);
        columnReader.read(ByteBuffer.wrap(content), encoding, 123+456, numBatches*numRows-123-456,
                10000, 123+456, targetVector, chunkIndex);

        for (int i = 0; i < numBatches*numRows; i++)
        {
            int j = i % numRows;
            assert targetVector.isNull[i] == originVector.isNull[j];
            if (targetVector.noNulls || !targetVector.isNull[i])
            {
                assert originVector.vector[j] == targetVector.vector[i];
            }
        }
    }
}
