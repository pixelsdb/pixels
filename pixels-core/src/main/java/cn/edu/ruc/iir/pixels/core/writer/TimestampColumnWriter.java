package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntEncoder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Timestamp column writer.
 * All timestamp values are converted to standard UTC time before they are stored as long values.
 * Currently not support nanos
 *
 * @author guodong
 */
public class TimestampColumnWriter extends BaseColumnWriter
{
    private final LongColumnVector curPixelTimeVector;

    public TimestampColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        curPixelTimeVector = new LongColumnVector(pixelStride);
        encoder = new RunLenIntEncoder(false, true);
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        TimestampColumnVector columnVector = (TimestampColumnVector) vector;
        long[] times = columnVector.time;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelEleCount + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelEleCount;
            System.arraycopy(times, curPartOffset, curPixelTimeVector.vector, curPixelEleCount, curPartLength);
            curPixelEleCount += curPartLength;
            newPixel(new boolean[0]);
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;

        System.arraycopy(times, curPartOffset, curPixelTimeVector.vector, curPixelEleCount, curPartLength);
        curPixelEleCount += curPartLength;

        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;

        if (nextPartLength > 0) {
            System.arraycopy(times, curPartOffset, curPixelTimeVector.vector, curPixelEleCount, nextPartLength);
            curPixelEleCount += nextPartLength;
        }

        return outputStream.size();
    }

    @Override
    public void newPixel(boolean[] isNull) throws IOException
    {
        for (int i = 0; i < curPixelEleCount; i++)
        {
            pixelStatRecorder.updateTimestamp(curPixelTimeVector.vector[i]);
        }

        if (isEncoding) {
            long[] values = new long[curPixelEleCount];
            for (int i = 0; i < curPixelEleCount; i++)
            {
                values[i] = curPixelTimeVector.vector[i];
            }
            outputStream.write(encoder.encode(values));
        }
        else {
            ByteBuffer curVecPartitionBuffer =
                    ByteBuffer.allocate(curPixelEleCount * Long.BYTES);
            for (int i = 0; i < curPixelEleCount; i++)
            {
                curVecPartitionBuffer.putLong(curPixelTimeVector.vector[i]);
            }
            outputStream.write(curVecPartitionBuffer.array());
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
