package cn.edu.ruc.iir.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong
 */
public interface BooleanColumnStats extends ColumnStats
{
    long getFalseCount();

    long getTrueCount();
}
