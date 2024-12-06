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
import io.pixelsdb.pixels.core.vector.LongDecimalColumnVector;
import io.pixelsdb.pixels.core.writer.LongDecimalColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestLongDecimalColumnReader
{
    @Test
    public void test() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        LongDecimalColumnWriter columnWriter = new LongDecimalColumnWriter(
                TypeDescription.createDecimal(38, 2), writerOption);
        LongDecimalColumnVector decimalColumnVector = new LongDecimalColumnVector(22, 38, 2);
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
        decimalColumnVector.add(3.58);
        decimalColumnVector.add(6.66);
        decimalColumnVector.add(7.77);
        decimalColumnVector.add(65656565.20);
        decimalColumnVector.add(3434.11);
        decimalColumnVector.add(54578.22);
        columnWriter.write(decimalColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        LongDecimalColumnReader columnReader = new LongDecimalColumnReader(
                TypeDescription.createDecimal(38, 2));
        LongDecimalColumnVector decimalColumnVector1 = new LongDecimalColumnVector(22, 38, 2);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                10, 0, decimalColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!decimalColumnVector1.noNulls && decimalColumnVector1.isNull[i])
            {
                assert !decimalColumnVector.noNulls && decimalColumnVector.isNull[i];
            }
            else
            {
                assert decimalColumnVector1.vector[i * 2] == decimalColumnVector.vector[i * 2];
                assert decimalColumnVector1.vector[i * 2 + 1] == decimalColumnVector.vector[i * 2 + 1];
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
        LongDecimalColumnWriter columnWriter = new LongDecimalColumnWriter(
                TypeDescription.createDecimal(38, 2), writerOption);

        LongDecimalColumnVector originVector = new LongDecimalColumnVector(rowNum, 38, 2);
        for (int j = 0; j < rowNum; j++)
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

        for (int i = 0; i < batchNum; i++)
        {
            columnWriter.write(originVector, rowNum);
        }
        columnWriter.flush();
        columnWriter.close();

        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        LongDecimalColumnReader columnReader = new LongDecimalColumnReader(
                TypeDescription.createDecimal(38, 2));
        LongDecimalColumnVector targetVector = new LongDecimalColumnVector(batchNum*rowNum, 38, 2);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, batchNum*rowNum,
                10000, 0, targetVector, chunkIndex);

        for (int i = 0; i < batchNum*rowNum; i++)
        {
            int j = i % rowNum;
            assert targetVector.isNull[i] == originVector.isNull[j];
            if (!targetVector.isNull[i])
            {
                assert originVector.vector[j * 2] == targetVector.vector[i * 2];
                assert originVector.vector[j * 2 + 1] == targetVector.vector[i * 2 + 1];
            }
        }
    }
}
