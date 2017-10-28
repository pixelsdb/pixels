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
    // todo what is pixel position recorded in column chunk index? absolute in pixel or in row group of in file?
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

        // temp holder of current partition buffer/values
        ByteBuffer curVecPartitionBuffer;
        long[] curVecPartitionValues;

        // do the calculation to partition the vector into current pixel and next one
        // doing this pre-calculation to eliminate branch prediction inside the for loop
        if ((curPixelEleCount + size) >= pixelStride) {
            curPartLength = pixelStride - curPixelEleCount;
            newPixelFlag = true;
        }
        else {
            curPartLength = size;
        }

        // update element count of current pixel
        curPixelEleCount += curPartLength;

        // update stats inside the for loop
        for (int i = 0; i < curPartLength; i++)
        {
            int value =(int) values[i + curPartOffset];
            pixelStatRecorder.update(value, 1);
        }

        // write out
        // 1. write out curPixelVector
        // 2. write out curVecPartitionBuffer/curVecPartitionValues
        // 3. update curPixelVector
        if (newPixelFlag)
        {
            if (isEncoding)
            {
                curVecPartitionValues = new long[curPartLength];
                System.arraycopy(values, curPartOffset, curVecPartitionValues, 0, curPartLength);
                outputStream.write(encoder.encode(curVecPartitionValues));
            } else
            {
                curVecPartitionBuffer = ByteBuffer.allocate(curPartLength * Integer.BYTES);
                for (int i = 0; i < curPartLength; i++)
                {
                    curVecPartitionBuffer.putInt((int) values[i + curPartOffset]);
                }
                outputStream.write(curVecPartitionBuffer.array());
            }

            // update position of current pixel
            curPixelPosition = outputStream.size();

            // reset and clean up. inline to remove function call newPixel().
            //
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

        return outputStream.size();
    }
}
