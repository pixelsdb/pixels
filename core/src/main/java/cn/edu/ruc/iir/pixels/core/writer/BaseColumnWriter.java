package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.Encoder;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class BaseColumnWriter implements ColumnWriter
{
    final int pixelStride;                     // indicate num of elements in a pixel
    final boolean isEncoding;                  // indicate if encoding enabled during writing
    final PixelsProto.ColumnChunkIndex.Builder columnChunkIndex;
    private final PixelsProto.ColumnStatistic.Builder columnChunkStat;

    final StatsRecorder pixelStatRecorder;
    final StatsRecorder columnChunkStatRecorder;

    int lastPixelPosition = 0;                 // ending offset of last pixel in the column chunk
    int curPixelPosition = 0;                  // current offset of this pixel in the column chunk. this is a relative value inside each column chunk.

    int curPixelEleCount = 0;                  // count of elements in previous vector

    Encoder encoder;

    final ByteArrayOutputStream outputStream;  // column chunk content

    public BaseColumnWriter(TypeDescription type, int pixelStride, boolean isEncoding)
    {
        this.pixelStride = pixelStride;
        this.isEncoding = isEncoding;

        this.columnChunkIndex =
                PixelsProto.ColumnChunkIndex.newBuilder();
        this.columnChunkStat =
                PixelsProto.ColumnStatistic.newBuilder();
        this.pixelStatRecorder = StatsRecorder.create(type);
        this.columnChunkStatRecorder = StatsRecorder.create(type);

        // todo a good estimation of chunk size is needed as the initial size of output stream
        this.outputStream = new ByteArrayOutputStream(pixelStride);
    }

    /**
     * Write ColumnVector
     *
     * Serialize vector into {@code ByteBufferOutputStream}.
     * Update pixel statistics and positions.
     * Update column chunk statistics.
     *
     * @param vector vector
     * @param size size of vector
     * @return size in bytes of current column chunk
     * */
    @Override
    public abstract int writeBatch(ColumnVector vector, int size) throws IOException;

    /**
     * Get byte array of column chunk content
     * */
    @Override
    public byte[] getColumnChunkContent()
    {
        return outputStream.toByteArray();
    }

    /**
     * Get column chunk size in bytes
     * */
    public int getColumnChunkSize()
    {
        return outputStream.size();
    }

    public PixelsProto.ColumnChunkIndex.Builder getColumnChunkIndex()
    {
        return columnChunkIndex;
    }

    public PixelsProto.ColumnStatistic.Builder getColumnChunkStat()
    {
        return columnChunkStatRecorder.serialize();
    }

    public StatsRecorder getColumnChunkStatRecorder()
    {
        return columnChunkStatRecorder;
    }

    public void newChunk()
    {
        if (curPixelEleCount > 0) {
            newPixel();
        }
    }

    public void reset()
    {
        lastPixelPosition = 0;
        curPixelPosition = 0;
        columnChunkIndex.clear();
        columnChunkStat.clear();
        pixelStatRecorder.reset();
        columnChunkStatRecorder.reset();
//        rowBatchBufferList.clear();
        outputStream.reset();
    }

    /**
     * End of a pixel
     * 1. set current pixel element count to 0 for the next batch pixel writing
     * 2. update column chunk stat
     * 3. add current pixel stat and position info to columnChunkIndex
     * 4. update lastPixelPosition to current one
     * 5. reset current pixel stat recorder
     */
    void newPixel()
    {
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
