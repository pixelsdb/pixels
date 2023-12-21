package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;
import io.pixelsdb.pixels.core.writer.DoubleColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.VectorColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestVectorColumnReader
{
    @Test
    public void test() throws IOException
    {
        // build the vector
        int length = 3;
        int dimension = 1;
        VectorColumnVector vectorColumnVector = new VectorColumnVector(length,dimension);
        vectorColumnVector.add(new double[]{1});
        vectorColumnVector.add(new double[]{0});
        vectorColumnVector.add(new double[]{1});

        // build column writer and write
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        VectorColumnWriter columnWriter = new VectorColumnWriter(
                TypeDescription.createVector(), writerOption);
        columnWriter.write(vectorColumnVector, length);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();

        // build column reader and read
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        VectorColumnReader columnReader = new VectorColumnReader(TypeDescription.createVector(), dimension);
        VectorColumnVector vectorColumnVector1 = new VectorColumnVector(length, dimension);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, length,
                10, 0, vectorColumnVector1, chunkIndex);
        for (int i = 0; i < length; ++i)
        {
            if (!vectorColumnVector1.noNulls && vectorColumnVector1.isNull[i])
            {
                assert !vectorColumnVector.noNulls && vectorColumnVector.isNull[i];
            }
            else
            {
                assert vectorColumnVector1.vector[i] == vectorColumnVector.vector[i];
            }
        }
    }

    @Test
    public void testDouble() throws IOException
    {
        int length = 3;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        DoubleColumnWriter columnWriter = new DoubleColumnWriter(
                TypeDescription.createDouble(), writerOption);
        DoubleColumnVector doubleColumnVector = new DoubleColumnVector(length);
        doubleColumnVector.add(1.0);
        doubleColumnVector.add(0.0);
        doubleColumnVector.add(1.0);
        columnWriter.write(doubleColumnVector, length);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        DoubleColumnReader columnReader = new DoubleColumnReader(TypeDescription.createDouble());
        DoubleColumnVector doubleColumnVector1 = new DoubleColumnVector(length);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, length,
                10, 0, doubleColumnVector1, chunkIndex);
        for (int i = 0; i < length; ++i)
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
}
