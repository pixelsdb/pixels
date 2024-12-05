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
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.writer.BooleanColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-18
 */
public class TestBooleanColumnReader
{
    @Test
    public void test() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(8).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        BooleanColumnWriter columnWriter = new BooleanColumnWriter(
                TypeDescription.createBoolean(), writerOption);
        ByteColumnVector byteColumnVector = new ByteColumnVector(22);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        columnWriter.write(byteColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        BooleanColumnReader columnReader = new BooleanColumnReader(TypeDescription.createBoolean());
        ByteColumnVector byteColumnVector1 = new ByteColumnVector(22);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                8, 0, byteColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!byteColumnVector1.noNulls && byteColumnVector1.isNull[i])
            {
                assert !byteColumnVector.noNulls && byteColumnVector.isNull[i];
            }
            else
            {
                assert byteColumnVector1.vector[i] == byteColumnVector.vector[i];
            }
        }
    }

    @Test
    public void testWithoutNullsPadding() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(8).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        BooleanColumnWriter columnWriter = new BooleanColumnWriter(
                TypeDescription.createBoolean(), writerOption);
        ByteColumnVector byteColumnVector = new ByteColumnVector(22);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(true);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.addNull();
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        byteColumnVector.add(false);
        byteColumnVector.add(false);
        byteColumnVector.add(true);
        columnWriter.write(byteColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        BooleanColumnReader columnReader = new BooleanColumnReader(TypeDescription.createBoolean());
        ByteColumnVector byteColumnVector1 = new ByteColumnVector(22);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                8, 0, byteColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!byteColumnVector1.noNulls && byteColumnVector1.isNull[i])
            {
                assert !byteColumnVector.noNulls && byteColumnVector.isNull[i];
            }
            else
            {
                assert byteColumnVector1.vector[i] == byteColumnVector.vector[i];
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
        BooleanColumnWriter columnWriter = new BooleanColumnWriter(
                TypeDescription.createBoolean(), writerOption);

        ByteColumnVector originVector = new ByteColumnVector(rowNum);
        for (int j = 0; j < rowNum; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(true);
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
        BooleanColumnReader columnReader = new BooleanColumnReader(TypeDescription.createBoolean());
        ByteColumnVector targetVector = new ByteColumnVector(batchNum*rowNum);
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
