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

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.core.utils.DatetimeUtils.localMillisToUtcDays;

/**
 * pixels
 *
 * 2021-04-25
 * @author hank
 */
public class DateStatsRecorder
        extends StatsRecorder implements DateColumnStats
{
    private boolean hasMinimum = false;
    private boolean hasMaximum = false;
    private int minimum = Integer.MIN_VALUE;
    private int maximum = Integer.MAX_VALUE;

    DateStatsRecorder() { }

    DateStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.DateStatistic dateState = statistic.getDateStatistics();
        if (dateState.hasMinimum())
        {
            minimum = dateState.getMinimum();
            hasMinimum = true;
        }
        if (dateState.hasMaximum())
        {
            maximum = dateState.getMaximum();
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
    public void updateDate(int value)
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
    public void updateDate(Date value)
    {
        this.updateDate(localMillisToUtcDays(value.getTime()));
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof DateColumnStats)
        {
            DateStatsRecorder dateStat = (DateStatsRecorder) other;
            if (dateStat.hasMinimum)
            {
                if (hasMinimum)
                {
                    if (dateStat.minimum < minimum)
                    {
                        minimum = dateStat.minimum;
                    }
                } else
                {
                    minimum = dateStat.minimum;
                    hasMinimum = true;
                }
            }
            if (dateStat.hasMaximum)
            {
                if (hasMaximum)
                {
                    if (dateStat.maximum > maximum)
                    {
                        maximum = dateStat.maximum;
                    }
                } else
                {
                    maximum = dateStat.maximum;
                    hasMaximum = true;
                }
            }
        }
        else
        {
            throw new IllegalArgumentException("Incompatible merging of date column statistics");
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.DateStatistic.Builder dateBuilder =
                PixelsProto.DateStatistic.newBuilder();
        if (hasMinimum)
        {
            dateBuilder.setMinimum(minimum);
        }
        if (hasMaximum)
        {
            dateBuilder.setMaximum(maximum);
        }
        builder.setDateStatistics(dateBuilder);
        builder.setNumberOfValues(numberOfValues);
        return builder;
    }

    @Override
    public Long getMinimum()
    {
        return (long) minimum;
    }

    @Override
    public Long getMaximum()
    {
        return (long) maximum;
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
        if (!(o instanceof DateStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        DateStatsRecorder that = (DateStatsRecorder) o;

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
        result = 15 * result + (int)(minimum ^ (minimum >>> 16));
        result = 15 * result + (int)(maximum ^ (maximum >>> 16));
        return result;
    }
}
