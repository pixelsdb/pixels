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
import io.pixelsdb.pixels.core.vector.DateColumnVector;
import io.pixelsdb.pixels.core.writer.DateColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-19 Zermatt
 */
public class TestDateColumnReader
{
    @Test
    public void test() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        DateColumnWriter columnWriter = new DateColumnWriter(
                TypeDescription.createDate(), writerOption);
        DateColumnVector dateColumnVector = new DateColumnVector(22);
        dateColumnVector.add(100);
        dateColumnVector.add(103);
        dateColumnVector.add(106);
        dateColumnVector.add(34);
        dateColumnVector.addNull();
        dateColumnVector.add(54);
        dateColumnVector.add(55);
        dateColumnVector.add(67);
        dateColumnVector.addNull();
        dateColumnVector.add(34);
        dateColumnVector.add(555);
        dateColumnVector.add(565);
        dateColumnVector.add(234);
        dateColumnVector.add(675);
        dateColumnVector.add(235);
        dateColumnVector.add(32434);
        dateColumnVector.add(3);
        dateColumnVector.add(6);
        dateColumnVector.add(7);
        dateColumnVector.add(65656565);
        dateColumnVector.add(3434);
        dateColumnVector.add(54578);
        columnWriter.write(dateColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DateColumnReader columnReader = new DateColumnReader(TypeDescription.createDate());
        DateColumnVector dateColumnVector1 = new DateColumnVector(22);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                10, 0, dateColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!dateColumnVector1.noNulls && dateColumnVector1.isNull[i])
            {
                assert !dateColumnVector.noNulls && dateColumnVector.isNull[i];
            }
            else
            {
                assert dateColumnVector1.dates[i] == dateColumnVector.dates[i];
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
        DateColumnWriter columnWriter = new DateColumnWriter(
                TypeDescription.createDate(), writerOption);

        DateColumnVector originVector = new DateColumnVector(rowNum);
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
        DateColumnReader columnReader = new DateColumnReader(TypeDescription.createDate());
        DateColumnVector targetVector = new DateColumnVector(batchNum*rowNum);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, batchNum*rowNum,
                10000, 0, targetVector, chunkIndex);

        for (int i = 0; i < batchNum*rowNum; i++)
        {
            assert targetVector.isNull[i] == originVector.isNull[i%rowNum];
            if (!targetVector.isNull[i])
            {
                assert targetVector.dates[i] == originVector.dates[i % rowNum];
            }
        }
    }
}
