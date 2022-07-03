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
import io.pixelsdb.pixels.core.utils.Integer128;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.core.utils.Integer128.*;

/**
 * @date 2022-07-03
 * @author hank
 */
public class Integer128StatsRecorder
        extends StatsRecorder implements Integer128ColumnStats
{
    private Integer128 minimum = MIN_VALUE;
    private Integer128 maximum = MAX_VALUE;
    private boolean hasMinimum = false;

    Integer128StatsRecorder()
    {
    }

    Integer128StatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.Integer128Statistic int128Stat = statistic.getInt128Statistics();
        long minHigh = MIN_HIGH, minLow = MIN_LOW;
        if (int128Stat.hasMinimumLow())
        {
            hasMinimum = true;
            minLow = int128Stat.getMinimumLow();
            minHigh = minLow >> 63;
        }
        if (int128Stat.hasMinimumHigh())
        {
            checkArgument(int128Stat.hasMinimumLow(),
                    "minimumLow must exist when minimumHigh exists");
            minHigh = int128Stat.getMinimumHigh();
        }
        this.minimum.update(minHigh, minLow);

        long maxHigh = MAX_HIGH, maxLow = MAX_LOW;
        if (int128Stat.hasMaximumLow())
        {
            maxLow = int128Stat.getMaximumLow();
            maxHigh = maxLow >> 63;
        }
        if (int128Stat.hasMaximumHigh())
        {
            checkArgument(int128Stat.hasMaximumLow(),
                    "maximumLow must exist when maximumHigh exists");
            maxHigh = int128Stat.getMaximumHigh();
        }
        this.maximum.update(maxHigh, maxLow);
    }

    @Override
    public void reset()
    {
        super.reset();
        hasMinimum = false;
        minimum = MIN_VALUE;
        maximum = MAX_VALUE;
    }

    @Override
    public void updateInteger128(Integer128 int128, int repetitions)
    {
        updateInteger128(int128.getHigh(), int128.getLow(), repetitions);
    }

    @Override
    public void updateInteger128(long high, long low, int repetitions)
    {
        numberOfValues += repetitions;
        if (!hasMinimum)
        {
            hasMinimum = true;
            minimum.update(high, low);
            maximum.update(high, low);
            return;
        }

        if (minimum.compareTo(high, low) > 0)
        {
            minimum.update(high, low);
        }
        else if (maximum.compareTo(high, low) < 0)
        {
            maximum.update(high, low);
        }
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof Integer128StatsRecorder)
        {
            Integer128StatsRecorder int128Stat = (Integer128StatsRecorder) other;
            if (!hasMinimum)
            {
                hasMinimum = int128Stat.hasMinimum;
                minimum = int128Stat.minimum;
                maximum = int128Stat.maximum;
            }
            else if (int128Stat.hasMinimum)
            {
                if (int128Stat.minimum.compareTo(this.minimum) < 0)
                {
                    minimum = int128Stat.minimum;
                }

                if (int128Stat.maximum.compareTo(this.maximum) > 0)
                {
                    maximum = int128Stat.maximum;
                }
            }
        }
        else
        {
            if (isStatsExists() && hasMinimum)
            {
                throw new IllegalArgumentException("Incompatible merging of integer128 column statistics");
            }
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.Integer128Statistic.Builder int128Builder =
                PixelsProto.Integer128Statistic.newBuilder();
        if (hasMinimum)
        {
            int128Builder.setMinimumHigh(minimum.getHigh());
            int128Builder.setMinimumLow(minimum.getLow());
            int128Builder.setMaximumHigh(maximum.getHigh());
            int128Builder.setMaximumLow(maximum.getLow());
        }
        builder.setInt128Statistics(int128Builder);
        builder.setNumberOfValues(numberOfValues);
        return builder;
    }

    @Override
    public Integer128 getMinimum()
    {
        return this.minimum;
    }

    @Override
    public Integer128 getMaximum()
    {
        return maximum;
    }

    @Override
    public boolean isSumDefined()
    {
        return false;
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
        buf.append(" numberOfValues: ")
                .append(numberOfValues);
        return buf.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof Integer128StatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        Integer128StatsRecorder that = (Integer128StatsRecorder) o;

        if (hasMinimum != that.hasMinimum)
        {
            return false;
        }
        if (!minimum.equals(that.minimum))
        {
            return false;
        }
        if (!maximum.equals(that.maximum))
        {
            return false;
        }
        return numberOfValues == that.numberOfValues;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + minimum.hashCode();
        result = 31 * result + maximum.hashCode();
        result = 31 * result + (int) (numberOfValues ^ (numberOfValues >>> 32));
        result = 31 * result + (hasMinimum ? 1 : 0);
        return result;
    }

}
