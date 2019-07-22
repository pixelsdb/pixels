package io.pixelsdb.pixels.core.stats;

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
