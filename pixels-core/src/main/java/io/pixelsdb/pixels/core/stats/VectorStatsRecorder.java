package io.pixelsdb.pixels.core.stats;

import io.pixelsdb.pixels.core.PixelsProto;

public class VectorStatsRecorder
        extends StatsRecorder implements VectorColumnStats {

    public VectorStatsRecorder() {

    }

    public VectorStatsRecorder(PixelsProto.ColumnStatistic statistic) {
        super(statistic);
    }

    @Override
    public void updateVector() {
        numberOfValues++;
    }
}
