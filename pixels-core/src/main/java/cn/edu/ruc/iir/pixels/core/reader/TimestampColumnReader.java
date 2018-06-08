package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntDecoder;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * pixels timestamp column reader
 * All timestamp values are translated to the specified time zone after read from file.
 *
 * @author guodong
 */
public class TimestampColumnReader
        extends ColumnReader
{
    private ByteBuf inputBuffer = null;
    private ByteBufInputStream inputStream = null;
    private RunLenIntDecoder decoder = null;
    private byte[] isNull;

    TimestampColumnReader(TypeDescription type)
    {
        super(type);
    }

    /**
     * Read values from input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param size     number of values to read
     * @param vector   vector to read into
     * @throws IOException
     */
    @Override
    public void read(byte[] input, PixelsProto.ColumnEncoding encoding, int isNullOffset,
                     int offset, int size, int pixelStride, ColumnVector vector) throws IOException
    {
        TimestampColumnVector columnVector = (TimestampColumnVector) vector;
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
            decoder = new RunLenIntDecoder(inputStream, false);
        }

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
                    columnVector.set(i + offset, new Timestamp(decoder.next()));
                }
            }
        }
        else
        {
            for (int i = 0; i < size; i++)
            {
                if (isNull[i] == 1)
                {
                    columnVector.isNull[i + offset] = true;
                }
                else
                {
                    columnVector.set(i + offset, new Timestamp(inputStream.readLong()));
                }
            }
        }
    }
}
