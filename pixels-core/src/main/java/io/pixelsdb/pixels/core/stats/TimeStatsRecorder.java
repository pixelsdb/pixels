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

/**
 * pixels
 *
 * 2021-04-25
 * @author hank
 */
public class TimeStatsRecorder
        extends StatsRecorder implements TimeColumnStats
{
    private boolean hasMinMax = false;
    private int minimum = Integer.MAX_VALUE;
    private int maximum = Integer.MIN_VALUE;

    TimeStatsRecorder()
    {
    }

    TimeStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.TimeStatistic tState = statistic.getTimeStatistics();
        if (tState.hasMinimum())
        {
            minimum = tState.getMinimum();
            hasMinMax = true;
        }
        if (tState.hasMaximum())
        {
            maximum = tState.getMaximum();
            hasMinMax = true;
        }
    }

    @Override
    public void reset()
    {
        super.reset();
        hasMinMax = false;
        minimum = Integer.MIN_VALUE;
        maximum = Integer.MAX_VALUE;
    }

    @Override
    public void updateTime(int value)
    {
        if (hasMinMax)
        {
            if (value < minimum)
            {
                minimum = value;
            }
            if (value > maximum)
            {
                maximum = value;
            }
        }
        else
        {
            minimum = maximum = value;
            hasMinMax = true;
        }
        numberOfValues++;
    }

    @Override
    public void updateTime(Time value)
    {
        if (hasMinMax)
        {
            if (value.getTime() < minimum)
            {
                minimum = (int) value.getTime();
            }
            if (value.getTime() > maximum)
            {
                maximum = (int) value.getTime();
            }
        }
        else
        {
            minimum = maximum = (int) value.getTime();
            hasMinMax = true;
        }
        numberOfValues++;
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof TimestampColumnStats)
        {
            TimeStatsRecorder tStat = (TimeStatsRecorder) other;
            if (hasMinMax)
            {
                if (tStat.getMinimum() < minimum)
                {
                    minimum = tStat.getMinimum();
                }
                if (tStat.getMaximum() > maximum)
                {
                    maximum = tStat.getMaximum();
                }
            }
            else
            {
                minimum = tStat.getMinimum();
                maximum = tStat.getMaximum();
            }
        }
        else
        {
            if (isStatsExists() && hasMinMax)
            {
                throw new IllegalArgumentException("Incompatible merging of time column statistics");
            }
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.TimeStatistic.Builder tBuilder =
                PixelsProto.TimeStatistic.newBuilder();
        if (hasMinMax)
        {
            tBuilder.setMinimum(minimum);
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
    public String toString()
    {
        StringBuilder buf = new StringBuilder(super.toString());
        if (hasMinMax)
        {
            buf.append(" min: ");
            buf.append(getMinimum());
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

        if (hasMinMax ? !(minimum == that.minimum) : that.hasMinMax)
        {
            return false;
        }
        if (hasMinMax ? !(maximum == that.maximum) : that.hasMinMax)
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
        result = 15 * result + (minimum ^ (minimum >>> 16));
        result = 15 * result + (maximum ^ (maximum >>> 16));
        return result;
    }
}
