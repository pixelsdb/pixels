package io.pixelsdb.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong
 */
public interface BooleanColumnStats
{
    long getFalseCount();

    long getTrueCount();
}
