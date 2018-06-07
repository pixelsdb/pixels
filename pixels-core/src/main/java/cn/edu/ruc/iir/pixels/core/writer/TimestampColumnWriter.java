package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntEncoder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
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
    private final long[] curPixelVector = new long[pixelStride];

    public TimestampColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
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

        while ((curPixelEleIndex + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelEleIndex;
            writeCurPartTime(columnVector, times, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartTime(columnVector, times, curPartLength, curPartOffset);

        return outputStream.size();
    }

    private void writeCurPartTime(TimestampColumnVector columnVector, long[] values, int curPartLength, int curPartOffset)
    {
        for (int i = 0; i < curPartLength; i++)
        {
            if (columnVector.isNull[i + curPartOffset])
            {
                hasNull = true;
            }
            else
            {
                curPixelVector[curPixelEleIndex++] = values[i + curPartOffset];
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }

    @Override
    public void newPixel() throws IOException
    {
        for (int i = 0; i < curPixelEleIndex; i++)
        {
            pixelStatRecorder.updateTimestamp(curPixelVector[i]);
        }

        if (isEncoding) {
            long[] values = new long[curPixelEleIndex];
            System.arraycopy(curPixelVector, 0, values, 0, curPixelEleIndex);
            outputStream.write(encoder.encode(values));
        }
        else {
            ByteBuffer curVecPartitionBuffer =
                    ByteBuffer.allocate(curPixelEleIndex * Long.BYTES);
            for (int i = 0; i < curPixelEleIndex; i++)
            {
                curVecPartitionBuffer.putLong(curPixelVector[i]);
            }
            outputStream.write(curVecPartitionBuffer.array());
        }

        super.newPixel();
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

    @Override
    public void close() throws IOException
    {
        encoder.close();
        super.close();
    }
}
