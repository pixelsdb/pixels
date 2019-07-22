package io.pixelsdb.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong
 */
public interface TimestampColumnStats extends RangeStats<Long>
{
    Long getMinimum();

    Long getMaximum();
}
