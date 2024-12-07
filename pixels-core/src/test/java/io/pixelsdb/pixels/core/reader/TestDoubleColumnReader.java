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
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.writer.DoubleColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestDoubleColumnReader
{
    @Test
    public void testNullsPadding() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        DoubleColumnWriter columnWriter = new DoubleColumnWriter(
                TypeDescription.createDouble(), writerOption);
        DoubleColumnVector doubleColumnVector = new DoubleColumnVector(22);
        doubleColumnVector.add(100.22);
        doubleColumnVector.add(103.32);
        doubleColumnVector.add(106.43);
        doubleColumnVector.add(34.10);
        doubleColumnVector.addNull();
        doubleColumnVector.add(54.09);
        doubleColumnVector.add(55.00);
        doubleColumnVector.add(67.23);
        doubleColumnVector.addNull();
        doubleColumnVector.add(34.58);
        doubleColumnVector.add(555.98);
        doubleColumnVector.add(565.76);
        doubleColumnVector.add(234.11);
        doubleColumnVector.add(675.34);
        doubleColumnVector.add(235.58);
        doubleColumnVector.add(32434.68);
        doubleColumnVector.add(3.58);
        doubleColumnVector.add(6.66);
        doubleColumnVector.add(7.77);
        doubleColumnVector.add(65656565.20);
        doubleColumnVector.add(3434.11);
        doubleColumnVector.add(54578.22);
        columnWriter.write(doubleColumnVector, 22);
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DoubleColumnReader columnReader = new DoubleColumnReader(TypeDescription.createDouble());
        DoubleColumnVector doubleColumnVector1 = new DoubleColumnVector(22);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                10, 0, doubleColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!doubleColumnVector1.noNulls && doubleColumnVector1.isNull[i])
            {
                assert !doubleColumnVector.noNulls && doubleColumnVector.isNull[i];
            }
            else
            {
                assert doubleColumnVector1.vector[i] == doubleColumnVector.vector[i];
            }
        }
    }

    @Test
    public void testLarge() throws IOException
    {
        int batchNum = 15;
        int rowNum = 1024;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10000).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        DoubleColumnWriter columnWriter = new DoubleColumnWriter(
                TypeDescription.createDouble(), writerOption);

        DoubleColumnVector originVector = new DoubleColumnVector(rowNum);
        for (int j = 0; j < rowNum; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000.0d);
            }
        }

        for (int i = 0; i < batchNum; i++)
        {
            columnWriter.write(originVector, rowNum);
        }
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DoubleColumnReader columnReader = new DoubleColumnReader(TypeDescription.createDouble());
        DoubleColumnVector targetVector = new DoubleColumnVector(batchNum*rowNum);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, batchNum*rowNum,
                10000, 0, targetVector, chunkIndex);

        for (int i = 0; i < batchNum*rowNum; i++)
        {
            assert targetVector.isNull[i] == originVector.isNull[i%rowNum];
            if (!targetVector.isNull[i])
            {
                assert targetVector.vector[i] == originVector.vector[i % rowNum];
            }
        }
    }
}
