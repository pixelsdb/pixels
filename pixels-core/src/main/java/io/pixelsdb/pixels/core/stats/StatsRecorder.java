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
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Integer128;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static io.pixelsdb.pixels.core.TypeDescription.MAX_SHORT_DECIMAL_PRECISION;

/**
 * This is a base class for recording (updating) all kinds of column statistics during file writing.
 *
 * @author guodong
 * @author hank
 */
public class StatsRecorder
        implements ColumnStats
{
    long numberOfValues = 0L;
    private boolean hasNull = false;

    StatsRecorder() { }

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

    public void updateInteger128(long high, long low, int repetitions)
    {
        throw new UnsupportedOperationException("Can't update integer128");
    }

    public void updateInteger128(Integer128 int128, int repetitions)
    {
        throw new UnsupportedOperationException("Can't update integer128");
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

    public void updateVector()
    {
        throw new UnsupportedOperationException("Can't update vector");
    }

    public void updateDate(Date value)
    {
        throw new UnsupportedOperationException("Can't update date");
    }

    public void updateDate(int value)
    {
        throw new UnsupportedOperationException("Can't update date");
    }

    public void updateTime(Time value)
    {
        throw new UnsupportedOperationException("Can't update time");
    }

    public void updateTime(int value)
    {
        throw new UnsupportedOperationException("Can't update time");
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

    public static StatsRecorder create(TypeDescription type)
    {
        switch (type.getCategory())
        {
            case BOOLEAN:
                return new BooleanStatsRecorder();
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new IntegerStatsRecorder();
            case DECIMAL:
                /**
                 * Issue #208:
                 * To be compatible with Presto, use IntegerColumnStats for decimal.
                 * Decimal and its statistics in Presto are represented as long. If
                 * needed in other places, integer statistics can be converted to double
                 * using the precision and scale from the type in the file footer.
                 */
                if (type.getPrecision() <= MAX_SHORT_DECIMAL_PRECISION)
                    return new IntegerStatsRecorder();
                else
                    return new Integer128StatsRecorder();
            case FLOAT:
            case DOUBLE:
                return new DoubleStatsRecorder();
            case STRING:
            case CHAR:
            case VARCHAR:
                return new StringStatsRecorder();
            case DATE:
                return new DateStatsRecorder();
            case TIME:
                return new TimeStatsRecorder();
            case TIMESTAMP:
                return new TimestampStatsRecorder();
            case BINARY:
            case VARBINARY:
                return new BinaryStatsRecorder();
            case VECTOR:
                return new VectorStatsRecorder();
            default:
                return new StatsRecorder();
        }
    }

    public static StatsRecorder create(TypeDescription type, PixelsProto.ColumnStatistic statistic)
    {
        switch (type.getCategory())
        {
            case BOOLEAN:
                return new BooleanStatsRecorder(statistic);
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new IntegerStatsRecorder(statistic);
            case DECIMAL:
                /**
                 * Issue #208:
                 * To be compatible with Presto, use IntegerColumnStats for decimal.
                 * Decimal and its statistics in Presto are represented as long. If
                 * needed in other places, integer statistics can be converted to double
                 * using the precision and scale from the type in the file footer.
                 */
                if (type.getPrecision() <= MAX_SHORT_DECIMAL_PRECISION)
                    return new IntegerStatsRecorder(statistic);
                else
                    return new Integer128StatsRecorder(statistic);
            case FLOAT:
            case DOUBLE:
                return new DoubleStatsRecorder(statistic);
            case STRING:
            case CHAR:
            case VARCHAR:
                return new StringStatsRecorder(statistic);
            case DATE:
                return new DateStatsRecorder(statistic);
            case TIME:
                return new TimeStatsRecorder(statistic);
            case TIMESTAMP:
                return new TimestampStatsRecorder(statistic);
            case BINARY:
            case VARBINARY:
                return new BinaryStatsRecorder(statistic);
            case VECTOR:
                return new VectorStatsRecorder(statistic);
            default:
                return new StatsRecorder(statistic);
        }
    }

    public static StatsRecorder create(TypeDescription.Category category, PixelsProto.ColumnStatistic statistic)
    {
        switch (category)
        {
            case BOOLEAN:
                return new BooleanStatsRecorder(statistic);
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new IntegerStatsRecorder(statistic);
            case DECIMAL:
                /**
                 * Issue #208:
                 * To be compatible with Presto, use IntegerColumnStats for decimal.
                 * Decimal and its statistics in Presto are represented as long. If
                 * needed in other places, integer statistics can be converted to double
                 * using the precision and scale from the type in the file footer.
                 */
                if (statistic.hasIntStatistics())
                    return new IntegerStatsRecorder(statistic);
                else
                    return new Integer128StatsRecorder(statistic);
            case FLOAT:
            case DOUBLE:
                return new DoubleStatsRecorder(statistic);
            case STRING:
            case CHAR:
            case VARCHAR:
                return new StringStatsRecorder(statistic);
            case DATE:
                return new DateStatsRecorder(statistic);
            case TIME:
                return new TimeStatsRecorder(statistic);
            case TIMESTAMP:
                return new TimestampStatsRecorder(statistic);
            case BINARY:
            case VARBINARY:
                return new BinaryStatsRecorder(statistic);
            case VECTOR:
                return new VectorStatsRecorder(statistic);
            default:
                return new StatsRecorder(statistic);
        }
    }

    /**
     * Cast the range stats to general range stats according to the type of the columns.
     *
     * @param type the type of the column
     * @param rangeStats the range stats to be cast
     * @return the general range stats if cast is successful, null otherwise
     */
    public static GeneralRangeStats toGeneral(TypeDescription type, RangeStats<?> rangeStats)
    {
        switch (type.getCategory())
        {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return GeneralRangeStats.fromStatsRecorder(type, (IntegerStatsRecorder) rangeStats);
            case DECIMAL:
                if (type.getPrecision() <= MAX_SHORT_DECIMAL_PRECISION)
                    return GeneralRangeStats.fromStatsRecorder(type, (IntegerStatsRecorder) rangeStats);
                else
                    return GeneralRangeStats.fromStatsRecorder(type, (Integer128StatsRecorder) rangeStats);
            case FLOAT:
            case DOUBLE:
                return GeneralRangeStats.fromStatsRecorder(type, (DoubleStatsRecorder) rangeStats);
            case DATE:
                return GeneralRangeStats.fromStatsRecorder(type, (DateStatsRecorder) rangeStats);
            case TIME:
                return GeneralRangeStats.fromStatsRecorder(type, (TimeStatsRecorder) rangeStats);
            case TIMESTAMP:
                return GeneralRangeStats.fromStatsRecorder(type, (TimestampStatsRecorder) rangeStats);
            default:
                // Other type are unknown or incompatible with general range stats.
                return null;
        }
    }
}
