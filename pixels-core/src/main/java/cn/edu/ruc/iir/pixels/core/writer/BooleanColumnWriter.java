package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.io.IOException;

/**
 * Boolean column writer.
 * Boolean values are compacted using bit-wise bytes, and then these integers are written out.
 *
 * @author guodong
 */
public class BooleanColumnWriter extends BaseColumnWriter
{
    private final long[] curPixelVector = new long[pixelStride];
    private final boolean[] isNull = new boolean[pixelStride];

    public BooleanColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        long[] values = columnVector.vector;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelEleCount + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelEleCount;
            for (int i = 0; i < curPartLength; i++) {
                long v = values[i + curPartOffset];
                curPixelVector[i + curPixelEleCount] = (v == 0 ? 0 : 1);
            }
            curPixelEleCount += curPartLength;
            System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelEleCount, curPartLength);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        for (int i = 0; i < curPartLength; i++) {
            long v = values[i + curPartOffset];
            curPixelVector[i + curPixelEleCount] = (v == 0 ? 0 : 1);
        }
        curPixelEleCount += curPartLength;
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;

        if (nextPartLength > 0) {
            for (int i = 0; i < curPartLength; i++) {
                long v = values[i + curPartOffset];
                curPixelVector[i + curPixelEleCount] = (v == 0 ? 0 : 1);
            }
            curPixelEleCount += nextPartLength;
        }

        return outputStream.size();
    }

    public void newPixel() throws IOException
    {
        for (int i = 0; i < curPixelEleCount; i++)
        {
            pixelStatRecorder.updateBoolean(curPixelVector[i] != 0, 1);
        }

        outputStream.write(BitUtils.bitWiseCompact(curPixelVector, curPixelEleCount));
        isNullStream.write(BitUtils.bitWiseCompact(isNull, curPixelEleCount));

        curPixelPosition = outputStream.size();
        curPixelEleCount = 0;
        columnChunkStatRecorder.merge(pixelStatRecorder);
        PixelsProto.PixelStatistic.Builder pixelStat =
                PixelsProto.PixelStatistic.newBuilder();
        pixelStat.setStatistic(pixelStatRecorder.serialize());
        columnChunkIndex.addPixelPositions(lastPixelPosition);
        columnChunkIndex.addPixelStatistics(pixelStat.build());
        lastPixelPosition = curPixelPosition;
        pixelStatRecorder.reset();
    }
}
