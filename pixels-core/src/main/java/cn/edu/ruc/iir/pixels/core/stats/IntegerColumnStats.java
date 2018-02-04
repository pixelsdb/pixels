package cn.edu.ruc.iir.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong
 */
public interface IntegerColumnStats extends RangeStats<Long>
{
    Long getMinimum();

    Long getMaximum();

    boolean isSumDefined();

    long getSum();
}
