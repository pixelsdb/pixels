package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenByteEncoder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class ByteColumnWriter extends BaseColumnWriter
{
    private final byte[] curPixelVector = new byte[pixelStride];

    public ByteColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        encoder = new RunLenByteEncoder();
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        long[] values = columnVector.vector;
        byte[] bvalues = new byte[size];
        for (int i = 0; i < size; i++)
        {
            bvalues[i] = (byte) values[i];
        }
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelEleCount + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelEleCount;
            System.arraycopy(bvalues, curPartOffset, curPixelVector, curPixelEleCount, curPartLength);
            curPixelEleCount += curPartLength;
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;

        System.arraycopy(bvalues, curPartOffset, curPixelVector, curPixelEleCount, curPartLength);
        curPixelEleCount += curPartLength;

        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;

        if (nextPartLength > 0) {
            System.arraycopy(bvalues,
                    curPartOffset,
                    curPixelVector,
                    curPixelEleCount,
                    nextPartLength);
            curPixelEleCount += nextPartLength;
        }

        return outputStream.size();
    }

    @Override
    public void newPixel() throws IOException
    {
        for (int i = 0; i < curPixelEleCount; i++)
        {
            pixelStatRecorder.updateInteger(curPixelVector[i], 1);
        }

        if (isEncoding) {
            outputStream.write(encoder.encode(curPixelVector));
        }
        else {
            outputStream.write(curPixelVector);
        }

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

    @Override
    public PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding()
    {
        if (isEncoding) {
            return PixelsProto.ColumnEncoding.newBuilder()
                    .setKind(PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
        }
        return PixelsProto.ColumnEncoding.newBuilder()
                .setKind(PixelsProto.ColumnEncoding.Kind.NONE);
    }
}
