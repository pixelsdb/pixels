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
    private int bitsIndex = 0;

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
    public void read(byte[] input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, ColumnVector vector,
                     PixelsProto.ColumnChunkIndex chunkIndex)
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        if (offset == 0)
        {
            // read isNull
            int isNullOffset = (int) chunkIndex.getIsNullOffset();
            byte[] isNullBytes = new byte[input.length - isNullOffset];
            ByteBuf inputBuf = Unpooled.wrappedBuffer(input);
            inputBuf.getBytes(isNullOffset, isNullBytes);
            inputBuf.release();
            isNull = BitUtils.bitWiseDeCompact(isNullBytes);
            // read content
            bits = BitUtils.bitWiseDeCompact(input);
            // re-init
            hasNull = true;
            elementIndex = 0;
            isNullIndex = 0;
            numOfPixelsWithoutNull = 0;
            bitsIndex = 0;
        }
        for (int i = 0; i < size; i++)
        {
            if (elementIndex % pixelStride == 0)
            {
                nextPixel(pixelStride, chunkIndex);
                bitsIndex = (int) Math.ceil((double) bitsIndex / 8.0d) * 8;
            }
            if (hasNull && isNull[isNullIndex++] == 1)
            {
                columnVector.isNull[i] = true;
            }
            else
            {
                columnVector.vector[i] = bits[bitsIndex++];
            }
            elementIndex++;
        }
    }
}
