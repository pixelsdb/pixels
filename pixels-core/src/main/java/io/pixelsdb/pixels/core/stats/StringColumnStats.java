package io.pixelsdb.pixels.core.stats;

import io.airlift.slice.Slice;

/**
 * pixels
 *
 * @author guodong
 */
public interface StringColumnStats extends RangeStats<Slice>
{
    Slice getMinimum();

    Slice getMaximum();

    /**
     * Get the total length of all strings
     *
     * @return the sum (total length)
     */
    long getSum();
}
