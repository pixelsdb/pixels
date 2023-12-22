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
import java.util.Arrays;
import java.util.Random;

public class TestVectorColumnReader
{
    @Test
    public void test() throws IOException
    {
        // build the vector
        int length = 3;
        int dimension = 1;
        VectorColumnVector vectorColumnVector = new VectorColumnVector(length, dimension);
        vectorColumnVector.add(new double[]{1});
        vectorColumnVector.add(new double[]{0});
        vectorColumnVector.add(new double[]{1});

        // build column writer and write
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        VectorColumnWriter columnWriter = new VectorColumnWriter(
                TypeDescription.createVector(dimension), writerOption);
        columnWriter.write(vectorColumnVector, length);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();

        // build column reader and read
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        VectorColumnReader columnReader = new VectorColumnReader(TypeDescription.createVector(dimension));
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
                assert Arrays.equals(vectorColumnVector1.vector[i], vectorColumnVector.vector[i]);
            }
        }
    }

    @Test
    public void testLargerVectors() throws IOException
    {
        // build the vector column vector
        int length = 1000;
        int dimension = 1024;
        VectorColumnVector vectorColumnVector = new VectorColumnVector(length, dimension);
        for (int i=0; i<length; i++) {
            vectorColumnVector.add(getRandomVector(dimension));
        }

        // build column writer and write
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(true);
        VectorColumnWriter columnWriter = new VectorColumnWriter(
                TypeDescription.createVector(dimension), writerOption);
        columnWriter.write(vectorColumnVector, length);
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();

        // build column reader and read
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        VectorColumnReader columnReader = new VectorColumnReader(TypeDescription.createVector(dimension));
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
                assert Arrays.equals(vectorColumnVector1.vector[i], vectorColumnVector.vector[i]);
            }
        }
    }

    public static double[] getRandomVector(int dimension)
    {
        Random random = new Random();
        double[] randomVec = new double[dimension];
        for (int i=0; i<dimension; i++) {
            randomVec[i] = random.nextDouble();
        }
        return randomVec;
    }

}
