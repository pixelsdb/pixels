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
import io.pixelsdb.pixels.core.utils.EncodingUtils;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * pixels
 *
 * @author guodong, hank
 */
public class StringStatsRecorder
        extends StatsRecorder implements StringColumnStats
{
    private String minimum = null;
    private String maximum = null;
    private long sum = 0L;

    StringStatsRecorder() { }

    StringStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.StringStatistic strStat = statistic.getStringStatistics();
        if (strStat.hasMinimum())
        {
            minimum = strStat.getMinimum();
        }
        if (strStat.hasMaximum())
        {
            maximum = strStat.getMaximum();
        }
        if (strStat.hasSum())
        {
            sum = strStat.getSum();
        }
    }

    @Override
    public void reset()
    {
        super.reset();
        minimum = null;
        maximum = null;
        sum = 0L;
    }

    @Override
    public void updateString(String value, int repetitions)
    {
        requireNonNull(value, "value is null");
        checkArgument(repetitions > 0, "repetitions is non-positive");
        if (minimum == null)
        {
            minimum = value;
        }
        else if (value.compareTo(minimum) < 0)
        {
            minimum = value;
        }
        if (maximum == null)
        {
            maximum = value;
        }
        else if (value.compareTo(maximum) > 0)
        {
            maximum = value;
        }
        sum += (long) value.length() * repetitions;
        numberOfValues += repetitions;
    }

    @Override
    public void updateString(byte[] value, int offset, int length, int repetitions)
    {
        ByteBuffer buffer = ByteBuffer.wrap(value, offset, length);
        try
        {
            String str = EncodingUtils.decodeString(buffer, true);
            updateString(str, repetitions);
        }
        catch (CharacterCodingException e)
        {
            buffer.clear();
        }
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof StringStatsRecorder)
        {
            StringStatsRecorder strStat = (StringStatsRecorder) other;
            if (strStat.minimum != null)
            {
                if (minimum == null)
                {
                    minimum = strStat.minimum;
                }
                else if (strStat.minimum.compareTo(minimum) < 0)
                {
                    minimum = strStat.minimum;
                }
            }
            if (strStat.maximum != null)
            {
                if (maximum == null)
                {
                    maximum = strStat.maximum;
                }
                else if (strStat.maximum.compareTo(maximum) < 0)
                {
                    maximum = strStat.maximum;
                }
            }
            sum += strStat.sum;
        }
        else
        {
            throw new IllegalArgumentException("Incompatible merging of string column statistics");
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.StringStatistic.Builder strBuilder =
                PixelsProto.StringStatistic.newBuilder();
        if (minimum != null)
        {
            strBuilder.setMinimum(minimum);

        }
        if (maximum != null)
        {
            strBuilder.setMaximum(maximum);
        }
        strBuilder.setSum(sum);
        builder.setStringStatistics(strBuilder);
        builder.setNumberOfValues(numberOfValues);
        return builder;
    }

    @Override
    public Slice getMinimum()
    {
        return Slices.utf8Slice(minimum);
    }

    @Override
    public Slice getMaximum()
    {
        return Slices.utf8Slice(maximum);
    }

    @Override
    public boolean hasMinimum()
    {
        return minimum != null;
    }

    @Override
    public boolean hasMaximum()
    {
        return maximum != null;
    }

    /**
     * Get the total length of all strings
     *
     * @return the sum (total length)
     */
    @Override
    public long getSum()
    {
        return sum;
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder(super.toString());
        if (minimum != null)
        {
            buf.append(" min: ");
            buf.append(getMinimum());
        }
        if (maximum != null)
        {
            buf.append(" max: ");
            buf.append(getMaximum());
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
        if (!(o instanceof StringStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        StringStatsRecorder that = (StringStatsRecorder) o;

        if (sum != that.sum)
        {
            return false;
        }
        if (!Objects.equals(minimum, that.minimum))
        {
            return false;
        }
        if (!Objects.equals(maximum, that.maximum))
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
        result = 31 * result + (minimum != null ? minimum.hashCode() : 0);
        result = 31 * result + (maximum != null ? maximum.hashCode() : 0);
        result = 31 * result + (int) (sum ^ (sum >>> 32));
        return result;
    }
}
