package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class BaseColumnWriter implements ColumnWriter
{
    final TypeDescription type;
    final int pixelStride;
    final PixelsProto.ColumnChunkIndex.Builder columnChunkIndex;
    final PixelsProto.ColumnStatistic.Builder columnChunkStat;

    final StatsRecorder pixelStatRecorder;
    final StatsRecorder columnChunkStatRecorder;

    final List<ByteBuffer> rowBatchBufferList;

    int curPixelSize = 0;
    int colChunkSize = 0;         // column chunk size in bytes

    public BaseColumnWriter(TypeDescription type, int pixelStride)
    {
        this.type = type;
        this.pixelStride = pixelStride;
        this.columnChunkIndex =
                PixelsProto.ColumnChunkIndex.newBuilder();
        this.columnChunkStat =
                PixelsProto.ColumnStatistic.newBuilder();

        this.pixelStatRecorder = StatsRecorder.create(type);
        this.columnChunkStatRecorder = StatsRecorder.create(type);

        this.rowBatchBufferList = new LinkedList<>();
    }

    // serialize vector into byte buffer, append to rowBatchBufferList
    // update pixel stat, add pixel positions, and update column chunk stat
    @Override
    public abstract int writeBatch(ColumnVector vector);

    public abstract byte[] serializeContent();

    public int getColumnChunkSize()
    {
        return colChunkSize;
    }

    public PixelsProto.ColumnChunkIndex.Builder getColumnChunkIndex()
    {
        return columnChunkIndex;
    }

    public PixelsProto.ColumnStatistic.Builder getColumnChunkStat()
    {
        return columnChunkStat;
    }

    public StatsRecorder getColumnChunkStatRecorder()
    {
        return columnChunkStatRecorder;
    }

    public void reset()
    {
        colChunkSize = 0;
        columnChunkIndex.clear();
        columnChunkStat.clear();
        pixelStatRecorder.reset();
        columnChunkStatRecorder.reset();
        rowBatchBufferList.clear();
    }

    /**
     * End of a pixel
     * 1. set current pixel size to 0 for the next batch pixel writing
     * 2. update column chunk stat
     * 3. add pixel stat to columnChunkIndex
     * 4. reset current pixel stat recorder
     */
    void newPixel()
    {
        curPixelSize = 0;
        columnChunkStatRecorder.merge(pixelStatRecorder);
        PixelsProto.PixelStatistic.Builder pixelStat =
                PixelsProto.PixelStatistic.newBuilder();
        pixelStat.setStatistic(pixelStatRecorder.serialize());
        // TODO columnChunkIndex add pixel positions
        columnChunkIndex.addPixelStatistics(pixelStat.build());
        pixelStatRecorder.reset();
    }
}
