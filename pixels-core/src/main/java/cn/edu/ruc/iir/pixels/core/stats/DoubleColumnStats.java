package cn.edu.ruc.iir.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong
 */
public interface DoubleColumnStats extends RangeStats<Double>
{
    Double getMinimum();

    Double getMaximum();

    double getSum();
}
