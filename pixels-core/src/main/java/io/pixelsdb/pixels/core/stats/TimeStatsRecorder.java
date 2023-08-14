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

import java.sql.Time;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels
 *
 * 2021-04-25
 * @author hank
 */
public class TimeStatsRecorder
        extends StatsRecorder implements TimeColumnStats
{
    private boolean hasMinimum = false;
    private boolean hasMaximum = false;
    private int minimum = Integer.MIN_VALUE;
    private int maximum = Integer.MAX_VALUE;

    TimeStatsRecorder() { }

    TimeStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.TimeStatistic tState = statistic.getTimeStatistics();
        if (tState.hasMinimum())
        {
            minimum = tState.getMinimum();
            hasMinimum = true;
        }
        if (tState.hasMaximum())
        {
            maximum = tState.getMaximum();
            hasMaximum = true;
        }
    }

    @Override
    public void reset()
    {
        super.reset();
        hasMinimum = false;
        hasMaximum = false;
        minimum = Integer.MIN_VALUE;
        maximum = Integer.MAX_VALUE;
    }

    @Override
    public void updateTime(int value)
    {
        if (hasMinimum)
        {
            if (value < minimum)
            {
                minimum = value;
            }
        }
        else
        {
            minimum = value;
            hasMinimum = true;
        }

        if (hasMaximum)
        {
            if (value > maximum)
            {
                maximum = value;
            }
        }
        else
        {
            maximum = value;
            hasMaximum = true;
        }
        numberOfValues++;
    }

    @Override
    public void updateTime(Time value)
    {
        this.updateTime((int) value.getTime());
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof TimeColumnStats)
        {
            TimeStatsRecorder tStat = (TimeStatsRecorder) other;
            if (tStat.hasMinimum)
            {
                if (hasMinimum)
                {
                    if (tStat.minimum < minimum)
                    {
                        minimum = tStat.minimum;
                    }
                }
                else
                {
                    minimum = tStat.minimum;
                    hasMinimum = true;
                }
            }

            if (tStat.hasMaximum)
            {
                if (hasMaximum)
                {
                    if (tStat.maximum > maximum)
                    {
                        maximum = tStat.maximum;
                    }
                }
                else
                {
                    maximum = tStat.maximum;
                    hasMaximum = true;
                }
            }
        }
        else
        {
            throw new IllegalArgumentException("Incompatible merging of time column statistics");
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.TimeStatistic.Builder tBuilder =
                PixelsProto.TimeStatistic.newBuilder();
        if (hasMinimum)
        {
            tBuilder.setMinimum(minimum);
        }
        if (hasMaximum)
        {
            tBuilder.setMaximum(maximum);
        }
        builder.setTimeStatistics(tBuilder);
        builder.setNumberOfValues(numberOfValues);
        return builder;
    }

    @Override
    public Integer getMinimum()
    {
        return minimum;
    }

    @Override
    public Integer getMaximum()
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
        int lower = minimum;
        int upper = maximum;
        if (lowerBound != null)
        {
            lower = (int) lowerBound;
        }
        if (upperBound != null)
        {
            upper = (int) upperBound;
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
        return (upper - lower) / (double) (maximum - minimum);
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder(super.toString());
        if (hasMinimum)
        {
            buf.append(" min: ");
            buf.append(getMinimum());
        }
        if (hasMaximum)
        {
            buf.append(" max: ");
            buf.append(getMaximum());
        }
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
        if (!(o instanceof TimeStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        TimeStatsRecorder that = (TimeStatsRecorder) o;

        if (hasMinimum ? !(minimum == that.minimum) : that.hasMinimum)
        {
            return false;
        }
        if (hasMaximum ? !(maximum == that.maximum) : that.hasMaximum)
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
        result = 15 * result + (hasMinimum ? 1 : 0);
        result = 15 * result + (hasMaximum ? 1 : 0);
        result = 15 * result + (minimum ^ (minimum >>> 16));
        result = 15 * result + (maximum ^ (maximum >>> 16));
        return result;
    }
}
