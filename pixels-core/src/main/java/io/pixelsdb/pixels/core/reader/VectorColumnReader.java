package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class VectorColumnReader extends ColumnReader {

    private ByteBuffer inputBuffer;
    private int inputIndex = 0;
    private int dimension;

    public VectorColumnReader(TypeDescription type) {
        super(type);
        this.dimension = type.getDimension();
    }

    /**
     * Read input buffer into a vector.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param offset   starting reading offset of values
     * @param size     number of values to read
     * @param pixelStride the stride (number of rows) in a pixel
     * @param vectorIndex the index from where we start reading values into the vector
     * @param vector   vector to read values into
     * @param chunkIndex the metadata of the column chunk to read.
     */
    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding, int offset, int size, int pixelStride, int vectorIndex, ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex) throws IOException
    {
        VectorColumnVector vectorColumnVector = (VectorColumnVector) vector;
        ((VectorColumnVector) vector).vector= new double[vector.getLength()][((VectorColumnVector) vector).dimension];
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        if (offset == 0)
        // initialize
        {
            this.inputBuffer = input;
            //this.inputBuffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
            inputIndex = inputBuffer.position();
            // isNull
            isNullOffset = inputIndex + chunkIndex.getIsNullOffset();
            // re-init
            hasNull = true;
            elementIndex = 0;
        }
        // elementIndex points at inputbuffer (?), and is about element instead of bytes (?)

        // read without copying the de-compacted content and isNull
        int numLeft = size, numToRead, bytesToDeCompact;
        for (int i = vectorIndex; numLeft > 0; )
        {
            if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
            {
                // read to the end of the current pixel
                numToRead = pixelStride - elementIndex % pixelStride;
            } else
            {
                numToRead = numLeft;
            }
            bytesToDeCompact = (numToRead + 7) / 8;
            // read isNull
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
                BitUtils.bitWiseDeCompact(vectorColumnVector.isNull, i, numToRead, inputBuffer, isNullOffset, littleEndian);
                isNullOffset += bytesToDeCompact;
                vectorColumnVector.noNulls = false;
            } else
            {
                Arrays.fill(vectorColumnVector.isNull, i, i + numToRead, false);
            }
            // read content
            if (nullsPadding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    for (int d=0; d<dimension; d++)
                    {
                        vectorColumnVector.vector[j][d] = inputBuffer.getDouble(inputIndex);
                        inputIndex += Double.BYTES;
                    }
                }
            } else
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    for (int d=0; d<dimension; d++)
                    {
                        vectorColumnVector.vector[j][d] = inputBuffer.getDouble(inputIndex);
                        inputIndex += Double.BYTES;
                    }
                }
            }
            // update variables
            numLeft -= numToRead;
            elementIndex += numToRead;
            i += numToRead;
        }
    }

    @Override
    public void close() throws IOException
    {

    }
}
