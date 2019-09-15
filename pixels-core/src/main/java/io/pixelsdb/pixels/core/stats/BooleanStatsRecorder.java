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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.stats;

import io.pixelsdb.pixels.core.PixelsProto;

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
