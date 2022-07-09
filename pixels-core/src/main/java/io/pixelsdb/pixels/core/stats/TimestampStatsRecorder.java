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

import static io.pixelsdb.pixels.core.vector.TimestampColumnVector.timestampToMicros;

/**
 * pixels
 *
 * @author guodong, hank
 */
public class TimestampStatsRecorder
        extends StatsRecorder implements TimestampColumnStats
{
    private boolean hasMinimum = false;
    private boolean hasMaximum = false;
    private long minimum = Long.MIN_VALUE;
    private long maximum = Long.MAX_VALUE;

    TimestampStatsRecorder() { }

    TimestampStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.TimestampStatistic tsState = statistic.getTimestampStatistics();
        if (tsState.hasMinimum())
        {
            minimum = tsState.getMinimum();
            hasMinimum = true;
        }
        if (tsState.hasMaximum())
        {
            maximum = tsState.getMaximum();
            hasMaximum = true;
        }
    }

    @Override
    public void reset()
    {
        super.reset();
        hasMinimum = false;
        hasMaximum = false;
        minimum = Long.MIN_VALUE;
        maximum = Long.MAX_VALUE;
    }

    @Override
    public void updateTimestamp(long value)
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
    public void updateTimestamp(Timestamp value)
    {
        this.updateTimestamp(timestampToMicros(value));
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof TimestampColumnStats)
        {
            TimestampStatsRecorder tsStat = (TimestampStatsRecorder) other;
            if (tsStat.hasMinimum)
            {
                if (hasMinimum)
                {
                    if (tsStat.minimum < minimum)
                    {
                        minimum = tsStat.minimum;
                    }
                } else
                {
                    hasMinimum = true;
                    minimum = tsStat.minimum;
                }
            }

            if (tsStat.hasMaximum)
            {
                if (hasMaximum)
                {
                    if (tsStat.maximum > maximum)
                    {
                        maximum = tsStat.maximum;
                    }
                } else
                {
                    hasMaximum = true;
                    maximum = tsStat.maximum;
                }
            }
        }
        else
        {
            throw new IllegalArgumentException("Incompatible merging of timestamp column statistics");
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.TimestampStatistic.Builder tsBuilder =
                PixelsProto.TimestampStatistic.newBuilder();
        if (hasMinimum)
        {
            tsBuilder.setMinimum(minimum);
        }
        if (hasMaximum)
        {
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
        if (!(o instanceof TimestampStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        TimestampStatsRecorder that = (TimestampStatsRecorder) o;

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
        result = 31 * result + (hasMinimum ? 1 : 0);
        result = 31 * result + (hasMaximum ? 1 : 0);
        result = 31 * result + (int) (minimum ^ (minimum >>> 32));
        result = 31 * result + (int) (maximum ^ (maximum >>> 32));
        return result;
    }
}
