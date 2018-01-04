package cn.edu.ruc.iir.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong
 */
public interface IntegerColumnStats extends ColumnStats
{
    long getMinimum();

    long getMaximum();

    boolean isSumDefined();

    long getSum();
}
