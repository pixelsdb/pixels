package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.UUID;

public class TestVectorColumnWriter {

    @Test
    public void test() throws IOException
    {
        int length = 5;
        int dimension = 2;
        VectorColumnVector vectorColumnVector = new VectorColumnVector(length, dimension);
        vectorColumnVector.reset();
        vectorColumnVector.init();
        for (int i = 0; i < length; ++i)
        {
            vectorColumnVector.add(getUniformVec(dimension, 1.1));
        }
        PixelsWriterOption pixelsWriterOption = new PixelsWriterOption()
                .pixelStride(10000).encodingLevel(EncodingLevel.EL2).byteOrder(ByteOrder.BIG_ENDIAN);
        VectorColumnWriter vectorColumnWriter = new VectorColumnWriter(
                TypeDescription.createVector(dimension), pixelsWriterOption);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < length; ++i)
        {
            vectorColumnWriter.write(vectorColumnVector, vectorColumnVector.getLength());
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }

    private double[] getUniformVec(int dimension, double val) {
        double[] vec = new double[dimension];
        for (int i=0; i<dimension; i++) {
            vec[i] = val;
        }
        return vec;
    }
}
