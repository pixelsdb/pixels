package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntEncoder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Integer column writer.
 * If encoding, use RunLength;
 * Else isLong(1 byte) + content
 *
 * @author guodong
 */
// todo fixing bug with signed int values encoding
public class IntegerColumnWriter extends BaseColumnWriter
{
    private final long[] curPixelVector = new long[pixelStride];        // current pixel value vector haven't written out yet
    private final boolean isLong;                         // current column type is long or int

    public IntegerColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        encoder = new RunLenIntEncoder();
        this.isLong = schema.getCategory() == TypeDescription.Category.LONG;
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        long[] values = columnVector.vector;
        int curPartLength;           // size of the partition which belongs to current pixel
        int curPartOffset = 0;           // starting offset of the partition which belongs to current pixel
        int nextPartLength = size;       // size of the partition which belongs to next pixel

        // do the calculation to partition the vector into current pixel and next one
        // doing this pre-calculation to eliminate branch prediction inside the for loop
        while ((curPixelEleCount + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelEleCount;
            // fill in current pixel value vector with current partition
            System.arraycopy(values, curPartOffset, curPixelVector, curPixelEleCount, curPartLength);
            curPixelEleCount += curPartLength;
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;

        // fill in current pixel value vector with current partition
        System.arraycopy(values, curPartOffset, curPixelVector, curPixelEleCount, curPartLength);
        curPixelEleCount += curPartLength;

        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;

        // update current pixel vector
        // actually this should never be reached!!!
        if (nextPartLength > 0) {
            System.arraycopy(values,
                    curPartOffset,
                    curPixelVector,
                    curPixelEleCount,
                    nextPartLength);
            curPixelEleCount += nextPartLength;
        }

        return outputStream.size();
    }

    @Override
    void newPixel() throws IOException
    {
        // update stats
        for (int i = 0; i < curPixelEleCount; i++)
        {
            pixelStatRecorder.updateInteger(curPixelVector[i], 1);
        }

        // write out current pixel vector
        if (isEncoding) {
            outputStream.write(encoder.encode(curPixelVector));
        }
        else {
            ByteBuffer curVecPartitionBuffer;
            if (isLong) {
                curVecPartitionBuffer = ByteBuffer.allocate(curPixelEleCount * Long.BYTES + 1);
                curVecPartitionBuffer.put((byte) 1);
                for (int i = 0; i < curPixelEleCount; i++)
                {
                    curVecPartitionBuffer.putLong(curPixelVector[i]);
                }
            }
            else {
                curVecPartitionBuffer = ByteBuffer.allocate(curPixelEleCount * Integer.BYTES + 1);
                curVecPartitionBuffer.put((byte) 0);
                for (int i = 0; i < curPixelEleCount; i++)
                {
                    curVecPartitionBuffer.putInt((int) curPixelVector[i]);
                }
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
}
