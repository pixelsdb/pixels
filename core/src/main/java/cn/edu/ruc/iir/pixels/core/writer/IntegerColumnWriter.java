package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RleEncoder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class IntegerColumnWriter extends BaseColumnWriter
{
    private final LongColumnVector curPixelVector;        // current pixel value vector haven't written out yet

    public IntegerColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        curPixelVector = new LongColumnVector(pixelStride);
        encoder = new RleEncoder();
    }

    @Override
    public int writeBatch(ColumnVector vector, int size) throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        long[] values = columnVector.vector;
        int curPartLength;               // size of the partition which belongs to current pixel
        int curPartOffset = 0;           // starting offset of the partition which belongs to current pixel
        boolean newPixelFlag = false;    // set to true if this batch write triggers a new pixel

        // do the calculation to partition the vector into current pixel and next one
        // doing this pre-calculation to eliminate branch prediction inside the for loop
        if ((curPixelEleCount + size) >= pixelStride) {
            curPartLength = pixelStride - curPixelEleCount;
            newPixelFlag = true;
        }
        else {
            curPartLength = size;
        }

        // fill in current pixel value vector with current partition
        System.arraycopy(values, 0, curPixelVector.vector, curPixelEleCount, curPartLength);
        curPixelEleCount += curPartLength;

        // write out a new pixel
        if (newPixelFlag)
        {
            // update stats
            for (int i = 0; i < curPixelEleCount; i++) {
                pixelStatRecorder.update((int) curPixelVector.vector[i], 1);
            }

            // write out current pixel vector
            if (isEncoding)
            {
                outputStream.write(encoder.encode(curPixelVector.vector));
            } else
            {
                ByteBuffer curVecPartitionBuffer = ByteBuffer.allocate(curPixelEleCount * Integer.BYTES);
                for (int i = 0; i < curPixelEleCount; i++)
                {
                    curVecPartitionBuffer.putInt((int) curPixelVector.vector[i]);
                }
                outputStream.write(curVecPartitionBuffer.array());
            }

            // update position of current pixel
            curPixelPosition = outputStream.size();

            // reset and clean up. inline to remove function call newPixel().
            // 1. set current pixel element count to 0 for the next batch pixel writing
            // 2. update column chunk stat
            // 3. add current pixel stat and position info to columnChunkIndex
            // 4. update lastPixelPosition to current one
            // 5. reset current pixel stat recorder
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

        // update current pixel vector
        System.arraycopy(values,
                curPartOffset + curPartLength,
                curPixelVector.vector,
                curPixelEleCount,
                size - curPartLength);
        curPixelEleCount += (size - curPartLength);

        return outputStream.size();
    }
}
