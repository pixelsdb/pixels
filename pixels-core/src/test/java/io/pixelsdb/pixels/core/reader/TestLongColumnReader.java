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
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.writer.IntegerColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-21
 */
public class TestLongColumnReader
{
    @Test
    public void testInt() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        IntegerColumnWriter columnWriter = new IntegerColumnWriter(
                TypeDescription.createInt(), writerOption);
        LongColumnVector longColumnVector = new LongColumnVector(22);
        longColumnVector.add(100);
        longColumnVector.add(103);
        longColumnVector.add(106);
        longColumnVector.add(34);
        longColumnVector.addNull();
        longColumnVector.add(54);
        longColumnVector.add(55);
        longColumnVector.add(67);
        longColumnVector.addNull();
        longColumnVector.add(34);
        longColumnVector.add(555);
        longColumnVector.add(565);
        longColumnVector.add(234);
        longColumnVector.add(675);
        longColumnVector.add(235);
        longColumnVector.add(32434);
        longColumnVector.add(3);
        longColumnVector.add(6);
        longColumnVector.add(7);
        longColumnVector.add(65656565);
        longColumnVector.add(3434);
        longColumnVector.add(54578);
        columnWriter.write(longColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        LongColumnReader columnReader = new LongColumnReader(TypeDescription.createInt());
        LongColumnVector longColumnVector1 = new LongColumnVector(22);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                10, 0, longColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!longColumnVector1.noNulls && longColumnVector1.isNull[i])
            {
                assert !longColumnVector.noNulls && longColumnVector.isNull[i];
            }
            else
            {
                assert longColumnVector1.vector[i] == longColumnVector.vector[i];
            }
        }
    }

    @Test
    public void testLong() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        IntegerColumnWriter columnWriter = new IntegerColumnWriter(
                TypeDescription.createLong(), writerOption);
        LongColumnVector longColumnVector = new LongColumnVector(22);
        longColumnVector.add(100);
        longColumnVector.add(103);
        longColumnVector.add(106);
        longColumnVector.add(34);
        longColumnVector.addNull();
        longColumnVector.add(54);
        longColumnVector.add(55);
        longColumnVector.add(67);
        longColumnVector.addNull();
        longColumnVector.add(34);
        longColumnVector.add(555);
        longColumnVector.add(565);
        longColumnVector.add(234);
        longColumnVector.add(675);
        longColumnVector.add(235);
        longColumnVector.add(32434);
        longColumnVector.add(3);
        longColumnVector.add(6);
        longColumnVector.add(7);
        longColumnVector.add(65656565);
        longColumnVector.add(3434);
        longColumnVector.add(54578);
        columnWriter.write(longColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        LongColumnReader columnReader = new LongColumnReader(TypeDescription.createLong());
        LongColumnVector longColumnVector1 = new LongColumnVector(22);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                10, 0, longColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!longColumnVector1.noNulls && longColumnVector1.isNull[i])
            {
                assert !longColumnVector.noNulls && longColumnVector.isNull[i];
            }
            else
            {
                assert longColumnVector1.vector[i] == longColumnVector.vector[i];
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
        IntegerColumnWriter columnWriter = new IntegerColumnWriter(
                TypeDescription.createLong(), writerOption);

        LongColumnVector originVector = new LongColumnVector(rowNum);
        for (int j = 0; j < rowNum; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000L);
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
        LongColumnReader columnReader = new LongColumnReader(TypeDescription.createLong());
        LongColumnVector targetVector = new LongColumnVector(batchNum*rowNum);
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
