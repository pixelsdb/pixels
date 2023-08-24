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
import io.pixelsdb.pixels.core.vector.FloatColumnVector;
import io.pixelsdb.pixels.core.writer.FloatColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author hank
 * @create 2023-08-20 Zermatt
 */
public class TestFloatColumnReader
{
    @Test
    public void test() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        FloatColumnWriter columnWriter = new FloatColumnWriter(
                TypeDescription.createFloat(), writerOption);
        FloatColumnVector floatColumnVector = new FloatColumnVector(22);
        floatColumnVector.add(100.22F);
        floatColumnVector.add(103.32F);
        floatColumnVector.add(106.43F);
        floatColumnVector.add(34.10F);
        floatColumnVector.addNull();
        floatColumnVector.add(54.09F);
        floatColumnVector.add(55.00F);
        floatColumnVector.add(67.23F);
        floatColumnVector.addNull();
        floatColumnVector.add(34.58F);
        floatColumnVector.add(555.98F);
        floatColumnVector.add(565.76F);
        floatColumnVector.add(234.11F);
        floatColumnVector.add(675.34F);
        floatColumnVector.add(235.58F);
        floatColumnVector.add(32434.68F);
        floatColumnVector.add(3.58F);
        floatColumnVector.add(6.66F);
        floatColumnVector.add(7.77F);
        floatColumnVector.add(65656565.20F);
        floatColumnVector.add(3434.11F);
        floatColumnVector.add(54578.22F);
        columnWriter.write(floatColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        FloatColumnReader columnReader = new FloatColumnReader(TypeDescription.createFloat());
        FloatColumnVector floatColumnVector1 = new FloatColumnVector(22);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                10, 0, floatColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!floatColumnVector1.noNulls && floatColumnVector1.isNull[i])
            {
                assert !floatColumnVector.noNulls && floatColumnVector.isNull[i];
            }
            else
            {
                assert floatColumnVector1.vector[i] == floatColumnVector.vector[i];
            }
        }
    }
}
