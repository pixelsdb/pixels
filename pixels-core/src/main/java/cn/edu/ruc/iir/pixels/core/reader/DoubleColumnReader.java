package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import cn.edu.ruc.iir.pixels.core.utils.EncodingUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class DoubleColumnReader
        extends ColumnReader
{
    private final EncodingUtils encodingUtils;
    private byte[] input;
    private byte[] isNull = new byte[8];
    private int isNullOffset = 0;
    private int isNullBitIndex = 0;
    private int inputIndex = 0;

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
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
    {
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        if (offset == 0)
        {
            this.input = input.array();
            inputIndex = 0;
            // isNull
            isNullOffset = (int) chunkIndex.getIsNullOffset();
            // re-init
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
                if (hasNull && isNullBitIndex > 0)
                {
                    BitUtils.bitWiseDeCompact(isNull, this.input, isNullOffset++, 1);
                    isNullBitIndex = 0;
                }
            }
            if (hasNull && isNullBitIndex >= 8)
            {
                BitUtils.bitWiseDeCompact(isNull, this.input, isNullOffset++, 1);
                isNullBitIndex = 0;
            }
            if (hasNull && isNull[isNullBitIndex] == 1)
            {
                columnVector.isNull[i + vectorIndex] = true;
                columnVector.noNulls = false;
            }
            else
            {
                columnVector.vector[i + vectorIndex] = encodingUtils.readLongLE(this.input, inputIndex);
                inputIndex += 8;
            }
            if (hasNull)
            {
                isNullBitIndex++;
            }
            elementIndex++;
        }
    }
}
