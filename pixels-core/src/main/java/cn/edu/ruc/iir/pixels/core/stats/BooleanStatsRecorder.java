package cn.edu.ruc.iir.pixels.core.stats;

import cn.edu.ruc.iir.pixels.core.PixelsProto;

/**
 * pixels
 *
 * @author guodong
 */
public class BooleanStatsRecorder
        extends StatsRecorder implements BooleanColumnStats
{
    private long trueCount = 0L;

    BooleanStatsRecorder()
    {
    }

    BooleanStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.BucketStatistic bkStat = statistic.getBucketStatistics();
        trueCount = bkStat.getCount(0);
    }

    @Override
    public void reset()
    {
        super.reset();
        trueCount = 0;
    }

    @Override
    public void updateBoolean(boolean value, int repetitions)
    {
        if (value)
        {
            trueCount += repetitions;
        }
        numberOfValues += repetitions;
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof BooleanColumnStats)
        {
            BooleanStatsRecorder statsRecorder = (BooleanStatsRecorder) other;
            this.trueCount += statsRecorder.trueCount;
        }
        else
        {
            if (isStatsExists() && trueCount != 0)
            {
                throw new IllegalArgumentException("Incompatible merging of boolean column statistics");
            }
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.BucketStatistic.Builder bktBuilder =
                PixelsProto.BucketStatistic.newBuilder();
        bktBuilder.addCount(trueCount);
        builder.setBucketStatistics(bktBuilder);
        builder.setNumberOfValues(numberOfValues);
        return builder;
    }

    @Override
    public long getTrueCount()
    {
        return trueCount;
    }

    @Override
    public long getFalseCount()
    {
        return getNumberOfValues() - trueCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof BooleanStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        BooleanStatsRecorder that = (BooleanStatsRecorder) o;

        if (trueCount != that.trueCount)
        {
            return false;
        }
        if (numberOfValues != that.numberOfValues)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (int) (trueCount ^ (trueCount >>> 32));
        return result;
    }
}
