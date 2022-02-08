/*
 * Copyright 2021 PixelsDB.
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

import java.sql.Date;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.millisToDay;

/**
 * pixels
 *
 * 2021-04-25
 * @author hank
 */
public class DateStatsRecorder
        extends StatsRecorder implements DateColumnStats
{
    private boolean hasMinMax = false;
    private long minimum = Integer.MAX_VALUE;
    private long maximum = Integer.MIN_VALUE;

    DateStatsRecorder()
    {
    }

    DateStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.DateStatistic dateState = statistic.getDateStatistics();
        if (dateState.hasMinimum())
        {
            minimum = dateState.getMinimum();
            hasMinMax = true;
        }
        if (dateState.hasMaximum())
        {
            maximum = dateState.getMaximum();
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
    public void updateDate(int value)
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
    public void updateDate(Date value)
    {
        if (hasMinMax)
        {
            int v = millisToDay(value.getTime());
            if (value.getTime() < minimum)
            {
                minimum = v;
            }
            if (value.getTime() > maximum)
            {
                maximum = v;
            }
        }
        else
        {
            minimum = maximum = millisToDay(value.getTime());
            hasMinMax = true;
        }
        numberOfValues++;
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof DateColumnStats)
        {
            DateStatsRecorder dateStat = (DateStatsRecorder) other;
            if (hasMinMax)
            {
                if (dateStat.getMinimum() < minimum)
                {
                    minimum = dateStat.getMinimum();
                }
                if (dateStat.getMaximum() > maximum)
                {
                    maximum = dateStat.getMaximum();
                }
            }
            else
            {
                minimum = dateStat.getMinimum();
                maximum = dateStat.getMaximum();
            }
        }
        else
        {
            if (isStatsExists() && hasMinMax)
            {
                throw new IllegalArgumentException("Incompatible merging of date column statistics");
            }
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.DateStatistic.Builder dateBuilder =
                PixelsProto.DateStatistic.newBuilder();
        dateBuilder.setMinimum((int)minimum);
        dateBuilder.setMaximum((int)maximum);
        builder.setDateStatistics(dateBuilder);
        builder.setNumberOfValues(numberOfValues);
        return builder;
    }

    @Override
    public Long getMinimum()
    {
        return minimum;
    }

    @Override
    public Long getMaximum()
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
        if (!(o instanceof DateStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        DateStatsRecorder that = (DateStatsRecorder) o;

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
        result = 15 * result + (int)(minimum ^ (minimum >>> 16));
        result = 15 * result + (int)(maximum ^ (maximum >>> 16));
        return result;
    }
}
