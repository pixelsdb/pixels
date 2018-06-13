package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntDecoder;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class IntegerColumnReader
        extends ColumnReader
{
    private RunLenIntDecoder decoder;
    private ByteBuf inputBuffer;
    private ByteBufInputStream inputStream;
    private byte[] isNull;

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
                     int offset, int size, int pixelStride, ColumnVector vector,
                     PixelsProto.ColumnChunkIndex chunkIndex)
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
            if (inputBuffer != null)
            {
                inputBuffer.release();
            }
            inputBuffer = Unpooled.wrappedBuffer(input);
            int isNullOffset = (int) chunkIndex.getIsNullOffset();
            byte[] isNullBytes = new byte[input.length - isNullOffset];
            inputBuffer.getBytes(isNullOffset, isNullBytes);
            isNull = BitUtils.bitWiseDeCompact(isNullBytes);
            inputStream = new ByteBufInputStream(inputBuffer);
            decoder = new RunLenIntDecoder(inputStream, true);
            hasNull = true;
            elementIndex = 0;
            isNullIndex = 0;
            numOfPixelsWithoutNull = 0;
        }
        // if run length encoded
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH))
        {
            for (int i = 0; i < size; i++)
            {
                if (elementIndex % pixelStride == 0)
                {
                    nextPixel(pixelStride, chunkIndex);
                }
                if (hasNull && isNull[isNullIndex++] == 1)
                {
                    columnVector.isNull[i] = true;
                    columnVector.add(0);
                }
                else
                {
                    columnVector.add(decoder.next());
                }
                elementIndex++;
            }
        }
        // if not encoded
        else
        {
            byte firstByte = inputStream.readByte();
            boolean isLong = firstByte == (byte) 1;
            // if long
            if (isLong)
            {
                for (int i = 0; i < size; i++)
                {
                    if (elementIndex % pixelStride == 0)
                    {
                        nextPixel(pixelStride, chunkIndex);
                    }
                    if (hasNull && isNull[isNullIndex++] == 1)
                    {
                        columnVector.isNull[i] = true;
                    }
                    else
                    {
                        columnVector.vector[i] = inputStream.readLong();
                    }
                    elementIndex++;
                }
            }
            // if int
            else {
                for (int i = 0; i < size; i++)
                {
                    if (elementIndex % pixelStride == 0)
                    {
                        nextPixel(pixelStride, chunkIndex);
                    }
                    if (hasNull && isNull[isNullIndex++] == 1)
                    {
                        columnVector.isNull[i] = true;
                    }
                    else
                    {
                        columnVector.vector[i] = inputStream.readInt();
                    }
                    elementIndex++;
                }
            }
        }
    }
}
