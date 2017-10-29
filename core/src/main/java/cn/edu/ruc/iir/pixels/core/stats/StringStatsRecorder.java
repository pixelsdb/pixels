package cn.edu.ruc.iir.pixels.core.stats;

import cn.edu.ruc.iir.pixels.core.PixelsProto;

/**
 * pixels
 *
 * @author guodong
 */
public class StringStatsRecorder extends StatsRecorder implements StringColumnStats
{
    private String minimum = null;
    private String maximum = null;
    private long sum = 0L;

    StringStatsRecorder() {}

    StringStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.StringStatistic strStat = statistic.getStringStatistics();
        if (strStat.hasMaximum()) {
            minimum = strStat.getMinimum();
        }
        if (strStat.hasMaximum()) {
            maximum = strStat.getMaximum();
        }
        if (strStat.hasSum()) {
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
        if (minimum == null) {
            minimum = maximum = value;
        }
        else {
            if (value.compareTo(minimum) < 0) {
                minimum = value;
            }
            if (value.compareTo(maximum) > 0) {
                maximum = value;
            }
        }
        sum += value.length() * repetitions;
        numberOfValues += repetitions;
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof StringStatsRecorder) {
            StringStatsRecorder strStat = (StringStatsRecorder) other;
            if (minimum == null) {
                if (strStat.minimum != null) {
                    minimum = strStat.minimum;
                    maximum = strStat.maximum;
                }
            }
            else {
                if (strStat.minimum != null) {
                    if (strStat.minimum.compareTo(minimum) < 0) {
                        minimum = strStat.minimum;
                    }
                    if (strStat.maximum.compareTo(maximum) > 0) {
                        maximum = strStat.maximum;
                    }
                }
            }
            sum += strStat.sum;
        }
        else {
            if (isStatsExists() && minimum != null) {
                throw new IllegalArgumentException("Incompatible merging of string column statistics");
            }
        }
        super.merge(other);
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.StringStatistic.Builder strBuilder =
                PixelsProto.StringStatistic.newBuilder();
        if (getNumberOfValues() != 0)
        {
            strBuilder.setMinimum(minimum);
            strBuilder.setMaximum(maximum);
            strBuilder.setSum(sum);
        }
        builder.setStringStatistics(strBuilder);
        builder.setNumberOfValues(numberOfValues);
        return builder;
    }

    @Override
    public String getMinimum()
    {
        return minimum;
    }

    @Override
    public String getMaximum()
    {
        return maximum;
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
    public String toString() {
        StringBuilder buf = new StringBuilder(super.toString());
        if (getNumberOfValues() != 0) {
            buf.append(" min: ");
            buf.append(getMinimum());
            buf.append(" max: ");
            buf.append(getMaximum());
            buf.append(" sum: ");
            buf.append(sum);
        }
        buf.append(" numberOfValues: ");
        buf.append(numberOfValues);
        return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StringStatsRecorder)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        StringStatsRecorder that = (StringStatsRecorder) o;

        if (sum != that.sum) {
            return false;
        }
        if (minimum != null ? !minimum.equals(that.minimum) : that.minimum != null) {
            return false;
        }
        if (maximum != null ? !maximum.equals(that.maximum) : that.maximum != null) {
            return false;
        }

        if (numberOfValues != that.numberOfValues) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (minimum != null ? minimum.hashCode() : 0);
        result = 31 * result + (maximum != null ? maximum.hashCode() : 0);
        result = 31 * result + (int) (sum ^ (sum >>> 32));
        return result;
    }
}
