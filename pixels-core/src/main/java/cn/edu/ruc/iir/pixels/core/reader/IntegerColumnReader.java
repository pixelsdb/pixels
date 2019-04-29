package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntDecoder;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class IntegerColumnReader
        extends ColumnReader
{
    private RunLenIntDecoder decoder;
    private ByteBuffer inputBuffer;
    private InputStream inputStream;
    private byte[] isNull;
    private int isNullOffset = 0;
    private int isNullBitIndex = 0;

    IntegerColumnReader(TypeDescription type)
    {
        super(type);
    }

    /**
     * Read input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param size     number of values to read
     * @param vector   vector to read into
     */
    @Override
    public void read(byte[] input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
            throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        // if read from start, init the stream and decoder
        if (offset == 0)
        {
            if (inputStream != null)
            {
                inputStream.close();
            }
            inputBuffer = ByteBuffer.wrap(input);
            inputStream = new ByteArrayInputStream(input);
            decoder = new RunLenIntDecoder(inputStream, true);
            // isNull
            isNullOffset = (int) chunkIndex.getIsNullOffset();
            // re-init
            hasNull = true;
            elementIndex = 0;
            isNullBitIndex = 8;
        }
        // if run length encoded
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH))
        {
            for (int i = 0; i < size; i++)
            {
                // if we're done with the current pixel, move to the next one
                if (elementIndex % pixelStride == 0)
                {
                    int pixelId = elementIndex / pixelStride;
                    hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                    if (hasNull && isNullBitIndex > 0)
                    {
                        isNull = BitUtils.bitWiseDeCompact(inputBuffer.array(), isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                }
                // if we're done with the current byte, move to the next one
                if (hasNull && isNullBitIndex >= 8)
                {
                    isNull = BitUtils.bitWiseDeCompact(inputBuffer.array(), isNullOffset++, 1);
                    isNullBitIndex = 0;
                }
                // check if current offset is null
                if (hasNull && isNull[isNullBitIndex] == 1)
                {
                    columnVector.isNull[i + vectorIndex] = true;
                    columnVector.noNulls = false;
                }
                else
                {
                    columnVector.vector[i + vectorIndex] = decoder.next();
                }
                if (hasNull)
                {
                    isNullBitIndex++;
                }
                elementIndex++;
            }
        }
        // if not encoded
        else
        {
            byte firstByte = inputBuffer.get();
            boolean isLong = firstByte == (byte) 1;
            // if long
            if (isLong)
            {
                for (int i = 0; i < size; i++)
                {
                    if (elementIndex % pixelStride == 0)
                    {
                        int pixelId = elementIndex / pixelStride;
                        hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                        if (hasNull && isNullBitIndex > 0)
                        {
                            isNull = BitUtils.bitWiseDeCompact(inputBuffer.array(), isNullOffset++, 1);
                            isNullBitIndex = 0;
                        }
                    }
                    if (hasNull && isNullBitIndex >= 8)
                    {
                        isNull = BitUtils.bitWiseDeCompact(inputBuffer.array(), isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                    if (hasNull && isNull[isNullBitIndex] == 1)
                    {
                        columnVector.isNull[i + vectorIndex] = true;
                        columnVector.noNulls = false;
                    }
                    else
                    {
                        columnVector.vector[i + vectorIndex] = inputBuffer.getLong();
                    }
                    if (hasNull)
                    {
                        isNullBitIndex++;
                    }
                    elementIndex++;
                }
            }
            // if int
            else
            {
                for (int i = 0; i < size; i++)
                {
                    if (elementIndex % pixelStride == 0)
                    {
                        int pixelId = elementIndex / pixelStride;
                        hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                        if (hasNull && isNullBitIndex > 0)
                        {
                            isNull = BitUtils.bitWiseDeCompact(inputBuffer.array(), isNullOffset++, 1);
                            isNullBitIndex = 0;
                        }
                    }
                    if (hasNull && isNullBitIndex >= 8)
                    {
                        isNull = BitUtils.bitWiseDeCompact(inputBuffer.array(), isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                    if (hasNull && isNull[isNullBitIndex] == 1)
                    {
                        columnVector.isNull[i + vectorIndex] = true;
                        columnVector.noNulls = false;
                    }
                    else
                    {
                        columnVector.vector[i + vectorIndex] = inputBuffer.getInt();
                    }
                    if (hasNull)
                    {
                        isNullBitIndex++;
                    }
                    elementIndex++;
                }
            }
        }
    }
}
