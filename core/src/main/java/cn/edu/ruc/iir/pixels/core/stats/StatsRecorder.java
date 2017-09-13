package cn.edu.ruc.iir.pixels.core.stats;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;

import java.sql.Timestamp;

/**
 * This is a base class for recording (updating) all kinds of column statistics during file writing.
 *
 * @author guodong
 */
public class StatsRecorder implements ColumnStats
{
    private long count = 0;
    private boolean hasNull = false;

    StatsRecorder() {}

    StatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        if (statistic.hasNumberOfValues()) {
            count = statistic.getNumberOfValues();
        }
        if (statistic.hasHasNull()) {
            hasNull = statistic.getHasNull();
        }
        else {
            hasNull = true;
        }
    }

    public void increment()
    {
        this.count += 1;
    }

    public void increment(int count)
    {
        this.count += count;
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

    public void updateBinary(byte[] bytes, int offset, int length,
                             int repetitions)
    {
        throw new UnsupportedOperationException("Can't update string");
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
        return (count > 0 || hasNull);
    }

    public void merge(StatsRecorder stats)
    {
        count += stats.count;
        hasNull |= stats.hasNull;
    }

    public void reset()
    {
        count = 0;
        hasNull = false;
    }

    public long getNumberOfValues() {
        return count;
    }

    public boolean hasNull() {
        return hasNull;
    }

    @Override
    public String toString() {
        return "count: " + count + " hasNull: " + hasNull;
    }

    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder =
                PixelsProto.ColumnStatistic.newBuilder();
        builder.setNumberOfValues(count);
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
