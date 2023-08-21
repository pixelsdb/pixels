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
    public void test() throws IOException
    {
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        StringColumnWriter columnWriter = new StringColumnWriter(
                TypeDescription.createString(), writerOption);
        BinaryColumnVector binaryColumnVector = new BinaryColumnVector(22);
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
        binaryColumnVector.add("3");
        binaryColumnVector.add("6");
        binaryColumnVector.add("7");
        binaryColumnVector.add("65656565");
        binaryColumnVector.add("3434");
        binaryColumnVector.add("54578");
        columnWriter.write(binaryColumnVector, 22);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        StringColumnReader columnReader = new StringColumnReader(TypeDescription.createString());
        BinaryColumnVector binaryColumnVector1 = new BinaryColumnVector(22);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, 22,
                10, 0, binaryColumnVector1, chunkIndex);
        for (int i = 0; i < 22; ++i)
        {
            if (!binaryColumnVector1.noNulls && binaryColumnVector1.isNull[i])
            {
                assert !binaryColumnVector.noNulls && binaryColumnVector.isNull[i];
            }
            else
            {
                String s1 = new String(binaryColumnVector1.vector[i], binaryColumnVector1.start[i], binaryColumnVector1.lens[i]);
                String s = new String(binaryColumnVector.vector[i], binaryColumnVector.start[i], binaryColumnVector.lens[i]);
                assert s1.equals(s);
            }
        }
    }
}
