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

/**
 * pixels
 *
 * @author guodong, hank
 */
public class IntegerStatsRecorder
        extends StatsRecorder implements IntegerColumnStats
{
    private long minimum = Long.MIN_VALUE;
    private long maximum = Long.MAX_VALUE;
    private long sum = 0L;
    private boolean hasMinimum = false;
    private boolean hasMaximum = false;
    private boolean overflow = false;

    IntegerStatsRecorder() { }

    IntegerStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.IntegerStatistic intStat = statistic.getIntStatistics();
        if (intStat.hasMinimum())
        {
            hasMinimum = true;
            minimum = intStat.getMinimum();
        }
        if (intStat.hasMaximum())
        {
            hasMaximum = true;
            maximum = intStat.getMaximum();
        }
        if (intStat.hasSum())
        {
            sum = intStat.getSum();
        }
        else
        {
            overflow = true;
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
        sum = 0L;
        overflow = false;
    }

    @Override
    public void updateInteger(long value, int repetitions)
    {
        numberOfValues += repetitions;
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
        if (!overflow)
        {
            boolean wasPositive = sum >= 0;
            sum += value * repetitions;
            if ((value >= 0) == wasPositive)
            {
                overflow = (sum >= 0) != wasPositive;
            }
        }
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof IntegerStatsRecorder)
        {
            IntegerStatsRecorder intStat = (IntegerStatsRecorder) other;
            if (intStat.hasMinimum)
            {
                if (!hasMinimum)
                {
                    hasMinimum = true;
                    minimum = intStat.minimum;
                }
                else if (intStat.minimum < minimum)
                {
                    minimum = intStat.minimum;
                }
            }
            if (intStat.hasMaximum)
            {
                if (!hasMaximum)
                {
                    hasMaximum = true;
                    maximum = intStat.maximum;
                }
                else if (intStat.maximum > maximum)
                {
                    maximum = intStat.maximum;
                }
            }

            overflow |= intStat.overflow;
            if (!overflow)
            {
                boolean wasPositive = sum >= 0;
                sum += intStat.sum;
                if ((intStat.sum >= 0) == wasPositive)
                {
                    overflow = (sum >= 0) != wasPositive;
                }
            }
        }
        else
        {
            if (isStatsExists() && hasMinimum)
            {
                throw new IllegalArgumentException("Incompatible merging of integer column statistics");
            }
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.IntegerStatistic.Builder intBuilder =
                PixelsProto.IntegerStatistic.newBuilder();
        if (hasMinimum)
        {
            intBuilder.setMinimum(minimum);
        }
        if (hasMaximum)
        {
            intBuilder.setMaximum(maximum);
        }
        if (!overflow)
        {
            intBuilder.setSum(sum);
        }
        builder.setIntStatistics(intBuilder);
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
    public boolean isSumDefined()
    {
        return !overflow;
    }

    @Override
    public long getSum()
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
        if (!overflow)
        {
            buf.append(" sum: ");
            buf.append(sum);
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
        if (!(o instanceof IntegerStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        IntegerStatsRecorder that = (IntegerStatsRecorder) o;

        if (hasMinimum != that.hasMinimum || hasMaximum != that.hasMaximum)
        {
            return false;
        }

        if (minimum != that.minimum)
        {
            return false;
        }
        if (maximum != that.maximum)
        {
            return false;
        }
        if (sum != that.sum)
        {
            return false;
        }

        if (overflow != that.overflow)
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
        result = 31 * result + (int) (sum ^ (sum >>> 32));
        result = 31 * result + (int) (numberOfValues ^ (numberOfValues >>> 32));
        result = 31 * result + (hasMinimum ? 1 : 0);
        result = 31 * result + (hasMaximum ? 1 : 0);
        result = 31 * result + (overflow ? 1 : 0);
        return result;
    }

}
