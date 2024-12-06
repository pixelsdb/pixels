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
import io.pixelsdb.pixels.core.vector.TimeColumnVector;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.TimeColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestTimeColumnReader
{
    @Test
    public void test() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);
        TimeColumnVector timeColumnVector = new TimeColumnVector(22, 3);
        timeColumnVector.add(100);
        timeColumnVector.add(103);
        timeColumnVector.add(106);
        timeColumnVector.add(34);
        timeColumnVector.addNull();
        timeColumnVector.add(54);
        timeColumnVector.add(55);
        timeColumnVector.add(67);
        timeColumnVector.addNull();
        timeColumnVector.add(34);
        timeColumnVector.add(555);
        timeColumnVector.add(565);
        timeColumnVector.add(234);
        timeColumnVector.add(675);
        timeColumnVector.add(235);
        timeColumnVector.add(32434);
        timeColumnVector.add(3);
        timeColumnVector.add(6);
        timeColumnVector.add(7);
        timeColumnVector.add(65656565);
        timeColumnVector.add(3434);
        timeColumnVector.add(54578);
        columnWriter.write(timeColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        TimeColumnReader columnReader = new TimeColumnReader(TypeDescription.createTime(3));
        TimeColumnVector timeColumnVector1 = new TimeColumnVector(22, 3);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                10, 0, timeColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!timeColumnVector1.noNulls && timeColumnVector1.isNull[i])
            {
                assert !timeColumnVector.noNulls && timeColumnVector.isNull[i];
            }
            else
            {
                assert timeColumnVector1.times[i] == timeColumnVector.times[i];
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
        TimeColumnWriter columnWriter = new TimeColumnWriter(
                TypeDescription.createTime(3), writerOption);

        TimeColumnVector originVector = new TimeColumnVector(rowNum, 3);
        for (int j = 0; j < rowNum; j++)
        {
            if (j % 100 == 0)
            {
                originVector.addNull();
            }
            else
            {
                originVector.add(1000);
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
        TimeColumnReader columnReader = new TimeColumnReader(TypeDescription.createTime(3));
        TimeColumnVector targetVector = new TimeColumnVector(batchNum*rowNum, 3);
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
