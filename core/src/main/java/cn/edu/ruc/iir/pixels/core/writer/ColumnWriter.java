package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

/**
 * pixels
 *
 * @author guodong
 */
public interface ColumnWriter
{
    void writeBatch(ColumnVector vector);
    byte[] serializeContent();
    int getColumnChunkSize();
    PixelsProto.ColumnChunkIndex.Builder getColumnChunkIndex();
    PixelsProto.ColumnStatistic.Builder getColumnChunkStat();
    void reset();
}
