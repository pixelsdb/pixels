package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import cn.edu.ruc.iir.pixels.core.vector.ByteColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class BooleanColumnReader
        extends ColumnReader
{
    private byte[] input;
    private byte[] bits;
    private byte[] isNull = new byte[8];
    private int bitsIndex = 0;
    private int isNullOffset = 0;
    private int isNullBitIndex = 0;

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
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
    {
        ByteColumnVector columnVector = (ByteColumnVector) vector;
        if (offset == 0)
        {
            bits = new byte[input.limit() * 8];
            // read content
            BitUtils.bitWiseDeCompact(bits, input.array(), input.arrayOffset(), input.limit());
            // read isNull
            isNullOffset = (int) chunkIndex.getIsNullOffset();
            this.input = input.array();
            // re-init
            bitsIndex = 0;
            hasNull = true;
            elementIndex = 0;
            isNullBitIndex = 8;
        }
        for (int i = 0; i < size; i++)
        {
            if (elementIndex % pixelStride == 0)
            {
                int pixelId = elementIndex / pixelStride;
                hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                // skip padding bits
                bitsIndex = (int) Math.ceil((double) bitsIndex / 8.0d) * 8;
                if (hasNull && isNullBitIndex > 0)
                {
                    BitUtils.bitWiseDeCompact(this.isNull, this.input, isNullOffset++, 1);
                    isNullBitIndex = 0;
                }
            }
            if (hasNull && isNullBitIndex >= 8)
            {
                BitUtils.bitWiseDeCompact(this.isNull, this.input, isNullOffset++, 1);
                isNullBitIndex = 0;
            }
            if (hasNull && isNull[isNullBitIndex] == 1)
            {
                columnVector.isNull[i + vectorIndex] = true;
                columnVector.noNulls = false;
            }
            else
            {
                columnVector.vector[i + vectorIndex] = bits[bitsIndex++];
            }
            if (hasNull)
            {
                isNullBitIndex++;
            }
            elementIndex++;
        }
    }
}
