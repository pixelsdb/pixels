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
    public void read(byte[] input, PixelsProto.ColumnEncoding encoding, int isNullOffset,
                     int offset, int size, int pixelStride, ColumnVector vector) throws IOException
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
            byte[] isNullBytes = new byte[input.length - isNullOffset];
            inputBuffer.getBytes(isNullOffset, isNullBytes);
            isNull = BitUtils.bitWiseDeCompact(isNullBytes, offset, size);
            inputStream = new ByteBufInputStream(inputBuffer);
            decoder = new RunLenIntDecoder(inputStream, true);
        }
        // if run length encoded
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH))
        {
            for (int i = 0; i < size; i++)
            {
                if (isNull[i] == 1)
                {
                    columnVector.isNull[i + offset] = true;
                }
                else
                {
                    columnVector.vector[i + offset] = decoder.next();
                }
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
                    if (isNull[i] == 1)
                    {
                        columnVector.isNull[i + offset] = true;
                    }
                    else
                    {
                        columnVector.vector[i + offset] = inputStream.readLong();
                    }
                }
            }
            // if int
            else {
                for (int i = 0; i < size; i++)
                {
                    if (isNull[i] == 1)
                    {
                        columnVector.isNull[i + offset] = true;
                    }
                    else
                    {
                        columnVector.vector[i + offset] = inputStream.readInt();
                    }
                }
            }
        }
    }
}
