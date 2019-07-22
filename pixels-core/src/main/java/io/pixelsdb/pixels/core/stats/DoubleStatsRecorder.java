package io.pixelsdb.pixels.core.stats;

import io.pixelsdb.pixels.core.PixelsProto;

/**
 * pixels
 *
 * @author guodong
 */
public class DoubleStatsRecorder
        extends StatsRecorder implements DoubleColumnStats
{
    private boolean hasMinimum = false;
    private double minimum = Double.MIN_VALUE;
    private double maximum = Double.MAX_VALUE;
    private double sum = 0;

    DoubleStatsRecorder()
    {
    }

    DoubleStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.DoubleStatistic dbStat = statistic.getDoubleStatistics();
        if (dbStat.hasMinimum())
        {
            hasMinimum = true;
            minimum = dbStat.getMinimum();
        }
        if (dbStat.hasMaximum())
        {
            maximum = dbStat.getMaximum();
        }
        if (dbStat.hasSum())
        {
            sum = dbStat.getSum();
        }
    }

    @Override
    public void reset()
    {
        super.reset();
        this.hasMinimum = false;
        this.minimum = Double.MIN_VALUE;
        this.maximum = Double.MAX_VALUE;
        this.sum = 0;
    }

    @Override
    public void updateDouble(double value)
    {
        if (!hasMinimum)
        {
            hasMinimum = true;
            minimum = value;
            maximum = value;
        }
        else if (value < minimum)
        {
            minimum = value;
        }
        else if (value > maximum)
        {
            maximum = value;
        }
        sum += value;
        numberOfValues++;
    }

    @Override
    public void updateFloat(float value)
    {
        if (!hasMinimum)
        {
            hasMinimum = true;
            minimum = value;
            maximum = value;
        }
        else if (value < minimum)
        {
            minimum = value;
        }
        else if (value > maximum)
        {
            maximum = value;
        }
        sum += value;
        numberOfValues++;
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof DoubleColumnStats)
        {
            DoubleStatsRecorder dbStat = (DoubleStatsRecorder) other;
            if (!hasMinimum)
            {
                hasMinimum = dbStat.hasMinimum;
                minimum = dbStat.minimum;
                maximum = dbStat.maximum;
            }
            else if (dbStat.minimum < minimum)
            {
                minimum = dbStat.minimum;
            }
            else if (dbStat.maximum > maximum)
            {
                maximum = dbStat.maximum;
            }
            sum += dbStat.sum;
        }
        else
        {
            if (isStatsExists() && hasMinimum)
            {
                throw new IllegalArgumentException("Incompatible merging of double column statistics");
            }
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.DoubleStatistic.Builder dbBuilder =
                PixelsProto.DoubleStatistic.newBuilder();
        if (hasMinimum)
        {
            dbBuilder.setMinimum(minimum);
            dbBuilder.setMaximum(maximum);
        }
        dbBuilder.setSum(sum);
        builder.setDoubleStatistics(dbBuilder);
        builder.setNumberOfValues(numberOfValues);
        return builder;
    }

    @Override
    public Double getMinimum()
    {
        return minimum;
    }

    @Override
    public Double getMaximum()
    {
        return maximum;
    }

    @Override
    public double getSum()
    {
        return sum;
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder(super.toString());
        if (hasMinimum)
        {
            buf.append(" min: ");
            buf.append(minimum);
            buf.append(" max: ");
            buf.append(maximum);
        }
        buf.append(" sum: ");
        buf.append(sum);
        buf.append(" numberOfValues: ");
        buf.append(numberOfValues);
        return buf.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof DoubleStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        DoubleStatsRecorder that = (DoubleStatsRecorder) o;

        if (hasMinimum != that.hasMinimum)
        {
            return false;
        }
        if (Double.compare(that.minimum, minimum) != 0)
        {
            return false;
        }
        if (Double.compare(that.maximum, maximum) != 0)
        {
            return false;
        }
        if (Double.compare(that.sum, sum) != 0)
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
        long temp;
        result = 31 * result + (hasMinimum ? 1 : 0);
        temp = Double.doubleToLongBits(minimum);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maximum);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(sum);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
