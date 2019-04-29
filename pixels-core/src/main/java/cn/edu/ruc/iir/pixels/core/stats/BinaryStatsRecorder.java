package cn.edu.ruc.iir.pixels.core.stats;

import cn.edu.ruc.iir.pixels.core.PixelsProto;

/**
 * pixels
 *
 * @author guodong
 */
public class BinaryStatsRecorder
        extends StatsRecorder implements BinaryColumnStats
{
    private long sum = 0L;

    BinaryStatsRecorder()
    {
    }

    BinaryStatsRecorder(PixelsProto.ColumnStatistic statistic)
    {
        super(statistic);
        PixelsProto.BinaryStatistic binaryStat = statistic.getBinaryStatistics();
        if (binaryStat.hasSum())
        {
            sum = binaryStat.getSum();
        }
    }

    @Override
    public void reset()
    {
        super.reset();
        this.sum = 0;
    }

    @Override
    public void updateBinary(byte[] bytes, int offset, int length, int repetitions)
    {
        sum += (long) length * repetitions;
        numberOfValues += repetitions;
    }

    @Override
    public void merge(StatsRecorder other)
    {
        if (other instanceof BinaryColumnStats)
        {
            BinaryStatsRecorder binaryStat = (BinaryStatsRecorder) other;
            this.sum += binaryStat.sum;
        }
        else
        {
            if (isStatsExists() && sum != 0)
            {
                throw new IllegalArgumentException("Incompatible merging of binary column statistics");
            }
        }
        super.merge(other);
    }

    /**
     * Get the total length of the binary blob
     *
     * @return sum
     */
    @Override
    public long getSum()
    {
        return this.sum;
    }

    @Override
    public PixelsProto.ColumnStatistic.Builder serialize()
    {
        PixelsProto.ColumnStatistic.Builder builder = super.serialize();
        PixelsProto.BinaryStatistic.Builder binaryBuilder = PixelsProto.BinaryStatistic.newBuilder();
        binaryBuilder.setSum(this.sum);
        builder.setBinaryStatistics(binaryBuilder);
        builder.setNumberOfValues(this.numberOfValues);
        return builder;
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder(super.toString());
        if (getNumberOfValues() != 0)
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
        if (!(o instanceof BinaryStatsRecorder))
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }

        BinaryStatsRecorder that = (BinaryStatsRecorder) o;

        if (sum != that.sum)
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
        result = 31 * result + (int) (sum ^ (sum >>> 32));
        return result;
    }
}
