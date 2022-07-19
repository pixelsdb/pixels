/*
 * Copyright 2017-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.stats;

import io.pixelsdb.pixels.core.PixelsProto;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels
 *
 * @author guodong, hank
 */
public class DoubleStatsRecorder
        extends StatsRecorder implements DoubleColumnStats
{
    private boolean hasMinimum = false;
    private boolean hasMaximum = false;
    private double minimum = Double.MIN_VALUE;
    private double maximum = Double.MAX_VALUE;
    private double sum = 0;

    DoubleStatsRecorder() { }

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
            hasMaximum = true;
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
        this.hasMaximum = false;
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
        }
        else if (value < minimum)
        {
            minimum = value;
        }
        if (!hasMaximum)
        {
            hasMaximum = true;
            maximum = value;
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
        this.updateDouble(value);
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof DoubleColumnStats)
        {
            DoubleStatsRecorder dbStat = (DoubleStatsRecorder) other;
            if (dbStat.hasMinimum)
            {
                if (!hasMinimum)
                {
                    hasMinimum = true;
                    minimum = dbStat.minimum;
                }
                else if (dbStat.minimum < minimum)
                {
                    minimum = dbStat.minimum;
                }
            }
            if (dbStat.hasMaximum)
            {
                if (!hasMaximum)
                {
                    hasMinimum = true;
                    maximum = dbStat.maximum;
                }
                else if (dbStat.maximum > maximum)
                {
                    maximum = dbStat.maximum;
                }
            }
            sum += dbStat.sum;
        }
        else
        {
            throw new IllegalArgumentException("Incompatible merging of double column statistics");
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
        }
        if (hasMaximum)
        {
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
    public boolean hasMinimum()
    {
        return hasMinimum;
    }

    @Override
    public boolean hasMaximum()
    {
        return hasMaximum;
    }

    @Override
    public double getSelectivity(Object lowerBound, boolean lowerInclusive, Object upperBound, boolean upperInclusive)
    {
        if (!this.hasMinimum || !this.hasMaximum)
        {
            return -1;
        }
        double lower = minimum;
        double upper = maximum;
        if (lowerBound != null)
        {
            lower = (double) lowerBound;
        }
        if (upperBound != null)
        {
            upper = (double) upperBound;
        }
        checkArgument(lower <= upper, "lower bound must be larger than the upper bound");
        if (lower < minimum)
        {
            lower = minimum;
        }
        if (upper > maximum)
        {
            upper = maximum;
        }
        return (upper - lower) / (maximum - minimum);
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
        }
        if (hasMaximum)
        {
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

        if (hasMinimum != that.hasMinimum || hasMaximum != that.hasMaximum)
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
        result = 31 * result + (hasMaximum ? 1 : 0);
        temp = Double.doubleToLongBits(minimum);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maximum);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(sum);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
