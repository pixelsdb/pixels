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
import io.pixelsdb.pixels.core.TypeDescription;

import java.sql.Timestamp;

/**
 * This is a base class for recording (updating) all kinds of column statistics during file writing.
 *
 * @author guodong
 */
public class StatsRecorder
        implements ColumnStats
{
    long numberOfValues = 0L;
    private boolean hasNull = false;

    StatsRecorder()
    {
    }

    StatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        if (statistic.hasNumberOfValues())
        {
            numberOfValues = statistic.getNumberOfValues();
        }
        if (statistic.hasHasNull())
        {
            hasNull = statistic.getHasNull();
        }
        else
        {
            hasNull = true;
        }
    }

    public void increment()
    {
        this.numberOfValues += 1;
    }

    public void increment(long count)
    {
        this.numberOfValues += count;
    }

    public void setHasNull()
    {
        this.hasNull = true;
    }

    public void updateBoolean(boolean value, int repetitions)
    {
        throw new UnsupportedOperationException("Can't update boolean");
    }

    public void updateInteger(long value, int repetitions)
    {
        throw new UnsupportedOperationException("Can't update integer");
    }

    public void updateFloat(float value)
    {
        throw new UnsupportedOperationException("Can't update float");
    }

    public void updateDouble(double value)
    {
        throw new UnsupportedOperationException("Can't update double");
    }

    public void updateString(String value, int repetitions)
    {
        throw new UnsupportedOperationException("Can't update string");
    }

    public void updateString(byte[] bytes, int offset, int length, int repetitions)
    {
        throw new UnsupportedOperationException("Can't update string");
    }

    public void updateBinary(byte[] bytes, int offset, int length,
                             int repetitions)
    {
        throw new UnsupportedOperationException("Can't update binary");
    }

    public void updateDate(int value)
    {
        throw new UnsupportedOperationException("Can't update date");
    }

    public void updateTimestamp(Timestamp value)
    {
        throw new UnsupportedOperationException("Can't update timestamp");
    }

    public void updateTimestamp(long value)
    {
        throw new UnsupportedOperationException("Can't update timestamp");
    }

    public boolean isStatsExists()
    {
        return (numberOfValues > 0 || hasNull);
    }

    public void merge(StatsRecorder stats)
    {
        numberOfValues += stats.numberOfValues;
        hasNull |= stats.hasNull;
    }

    public void reset()
    {
        numberOfValues = 0;
        hasNull = false;
    }

    public long getNumberOfValues()
    {
        return numberOfValues;
    }

    public boolean hasNull()
    {
        return hasNull;
    }

    @Override
    public String toString()
    {
        return "numberOfValues: " + numberOfValues + " hasNull: " + hasNull;
    }

    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder =
                PixelsProto.ColumnStatistic.newBuilder();
        builder.setNumberOfValues(numberOfValues);
        builder.setHasNull(hasNull);
        return builder;
    }

    public static StatsRecorder create(TypeDescription schema)
    {
        switch (schema.getCategory())
        {
            case BOOLEAN:
                return new BooleanStatsRecorder();
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new IntegerStatsRecorder();
            case FLOAT:
            case DOUBLE:
                return new DoubleStatsRecorder();
            case STRING:
            case CHAR:
            case VARCHAR:
                return new StringStatsRecorder();
            case TIMESTAMP:
                return new TimestampStatsRecorder();
            case BINARY:
                return new BinaryStatsRecorder();
            default:
                return new StatsRecorder();
        }
    }

    public static StatsRecorder create(TypeDescription schema, PixelsProto.ColumnStatistic statistic)
    {
        switch (schema.getCategory())
        {
            case BOOLEAN:
                return new BooleanStatsRecorder(statistic);
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new IntegerStatsRecorder(statistic);
            case FLOAT:
            case DOUBLE:
                return new DoubleStatsRecorder(statistic);
            case STRING:
            case CHAR:
            case VARCHAR:
                return new StringStatsRecorder(statistic);
            case TIMESTAMP:
                return new TimestampStatsRecorder(statistic);
            case BINARY:
                return new BinaryStatsRecorder(statistic);
            default:
                return new StatsRecorder(statistic);
        }
    }
}
