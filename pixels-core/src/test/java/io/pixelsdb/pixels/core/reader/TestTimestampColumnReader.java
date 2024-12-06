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
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.TimestampColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestTimestampColumnReader
{
    @Test
    public void test() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimestampColumnWriter columnWriter = new TimestampColumnWriter(
                TypeDescription.createTimestamp(6), writerOption);
        TimestampColumnVector timestampColumnVector = new TimestampColumnVector(22, 6);
        timestampColumnVector.add(100L);
        timestampColumnVector.add(103L);
        timestampColumnVector.add(106L);
        timestampColumnVector.add(34L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(54L);
        timestampColumnVector.add(55L);
        timestampColumnVector.add(67L);
        timestampColumnVector.addNull();
        timestampColumnVector.add(34L);
        timestampColumnVector.add(555L);
        timestampColumnVector.add(565L);
        timestampColumnVector.add(234L);
        timestampColumnVector.add(675L);
        timestampColumnVector.add(235L);
        timestampColumnVector.add(32434L);
        timestampColumnVector.add(3L);
        timestampColumnVector.add(6L);
        timestampColumnVector.add(7L);
        timestampColumnVector.add(65656565L);
        timestampColumnVector.add(3434L);
        timestampColumnVector.add(54578L);
        columnWriter.write(timestampColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimestampColumnReader columnReader = new TimestampColumnReader(TypeDescription.createTimestamp(6));
        TimestampColumnVector timestampColumnVector1 = new TimestampColumnVector(22, 6);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                10, 0, timestampColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!timestampColumnVector1.noNulls && timestampColumnVector1.isNull[i])
            {
                assert !timestampColumnVector.noNulls && timestampColumnVector.isNull[i];
            }
            else
            {
                assert timestampColumnVector1.times[i] == timestampColumnVector.times[i];
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
        TimestampColumnWriter columnWriter = new TimestampColumnWriter(
                TypeDescription.createTimestamp(6), writerOption);

        TimestampColumnVector originVector = new TimestampColumnVector(rowNum, 6);
        for (int j = 0; j < rowNum; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            } else
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
        TimestampColumnReader columnReader = new TimestampColumnReader(TypeDescription.createTimestamp(6));
        TimestampColumnVector targetVector = new TimestampColumnVector(batchNum*rowNum, 6);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, batchNum*rowNum,
                10000, 0, targetVector, chunkIndex);

        for (int i = 0; i < batchNum*rowNum; i++)
        {
            assert targetVector.isNull[i] == originVector.isNull[i%rowNum];
            if (!targetVector.isNull[i])
            {
                assert targetVector.times[i] == originVector.times[i % rowNum];
            }
        }
    }
}
