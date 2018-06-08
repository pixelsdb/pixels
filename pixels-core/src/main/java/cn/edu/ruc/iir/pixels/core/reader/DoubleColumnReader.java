package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import cn.edu.ruc.iir.pixels.core.utils.EncodingUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * pixels
 *
 * @author guodong
 */
public class DoubleColumnReader
        extends ColumnReader
{
    private final EncodingUtils encodingUtils;
    private ByteBuf inputBuffer;
    private byte[] isNull;

    DoubleColumnReader(TypeDescription type)
    {
        super(type);
        this.encodingUtils = new EncodingUtils();
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
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        if (offset == 0)
        {
            if (inputBuffer != null)
            {
                inputBuffer.release();
            }
            inputBuffer = Unpooled.wrappedBuffer(input);
            byte[] isNullBytes = new byte[input.length - isNullOffset];
            inputBuffer.getBytes(isNullOffset, isNullBytes);
            isNull = BitUtils.bitWiseDeCompact(isNullBytes, offset, size);
        }
        for (int i = 0; i < size; i++)
        {
            if (isNull[i] == 1)
            {
                columnVector.isNull[i + offset] = true;
            }
            else
            {
                byte[] inputBytes = new byte[8];
                inputBuffer.readBytes(inputBytes);
                columnVector.vector[i + offset] = Double.longBitsToDouble(encodingUtils.readLongLE(inputBytes));
            }
        }
    }
}
