package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.vector.ColumnVector;

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
