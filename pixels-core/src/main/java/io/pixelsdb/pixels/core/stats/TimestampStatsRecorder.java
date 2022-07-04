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

import java.sql.Timestamp;

/**
 * pixels
 *
 * @author guodong
 */
public class TimestampStatsRecorder
        extends StatsRecorder implements TimestampColumnStats
{
    // Issue #94: change hasMinimum to hasMinMax according to its semantic.
    private boolean hasMinMax = false;
    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;

    TimestampStatsRecorder()
    {
    }

    TimestampStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.TimestampStatistic tsState = statistic.getTimestampStatistics();
        if (tsState.hasMinimum())
        {
            minimum = tsState.getMinimum();
            hasMinMax = true;
        }
        if (tsState.hasMaximum())
        {
            maximum = tsState.getMaximum();
            hasMinMax = true;
        }
    }

    @Override
    public void reset()
    {
        super.reset();
        hasMinMax = false;
        minimum = Long.MIN_VALUE;
        maximum = Long.MAX_VALUE;
    }

    @Override
    public void updateTimestamp(long value)
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
    public void updateTimestamp(Timestamp value)
    {
        if (hasMinMax)
        {
            if (value.getTime() < minimum)
            {
                minimum = value.getTime();
            }
            if (value.getTime() > maximum)
            {
                maximum = value.getTime();
            }
        }
        else
        {
            minimum = maximum = value.getTime();
            hasMinMax = true;
        }
        numberOfValues++;
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof TimestampColumnStats)
        {
            TimestampStatsRecorder tsStat = (TimestampStatsRecorder) other;
            if (hasMinMax)
            {
                if (tsStat.getMinimum() < minimum)
                {
                    minimum = tsStat.getMinimum();
                }
                if (tsStat.getMaximum() > maximum)
                {
                    maximum = tsStat.getMaximum();
                }
            }
            else
            {
                minimum = tsStat.getMinimum();
                maximum = tsStat.getMaximum();
            }
        }
        else
        {
            if (isStatsExists() && hasMinMax)
            {
                throw new IllegalArgumentException("Incompatible merging of timestamp column statistics");
            }
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.TimestampStatistic.Builder tsBuilder =
                PixelsProto.TimestampStatistic.newBuilder();
        if (hasMinMax)
        {
            tsBuilder.setMinimum(minimum);
            tsBuilder.setMaximum(maximum);
        }
        builder.setTimestampStatistics(tsBuilder);
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
        if (!(o instanceof TimestampStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        TimestampStatsRecorder that = (TimestampStatsRecorder) o;

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
        result = 31 * result + (int) (minimum ^ (minimum >>> 32));
        result = 31 * result + (int) (maximum ^ (maximum >>> 32));
        return result;
    }
}
