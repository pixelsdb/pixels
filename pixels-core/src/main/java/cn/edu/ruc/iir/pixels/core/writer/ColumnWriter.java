package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public interface ColumnWriter
{
    int write(ColumnVector vector, int length)
            throws IOException;

    byte[] getColumnChunkContent();

    int getColumnChunkSize();

    PixelsProto.ColumnChunkIndex.Builder getColumnChunkIndex();

    PixelsProto.ColumnStatistic.Builder getColumnChunkStat();

    PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding();

    StatsRecorder getColumnChunkStatRecorder();

    void reset();

    void flush()
            throws IOException;

    void close()
            throws IOException;
}
