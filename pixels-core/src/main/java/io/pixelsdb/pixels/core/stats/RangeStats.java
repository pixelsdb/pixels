package io.pixelsdb.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong
 */
public interface RangeStats<T>
{
    T getMinimum();

    T getMaximum();
}
