package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * pixels
 *
 * @author guodong
 */
public class BooleanColumnReader
        extends ColumnReader
{
    private byte[] bits;
    private byte[] isNull;

    BooleanColumnReader(TypeDescription type)
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
                     int offset, int size, int pixelStride, ColumnVector vector)
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        if (offset == 0)
        {
            byte[] isNullBytes = new byte[input.length - isNullOffset];
            ByteBuf inputBuf = Unpooled.wrappedBuffer(input);
            inputBuf.getBytes(isNullOffset, isNullBytes);
            inputBuf.release();

            // read isNull
            isNull = BitUtils.bitWiseDeCompact(isNullBytes, offset, size);
            // read content
            bits = BitUtils.bitWiseDeCompact(input);
        }
        for (int i = 0; i < size; i++)
        {
            if (isNull[i] == 1)
            {
                columnVector.isNull[i + offset] = true;
            }
            else
            {
                columnVector.vector[i] = bits[i + offset] == 1 ? 1 : 0;
            }
        }
    }
}
