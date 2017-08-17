package cn.edu.ruc.iir.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong
 */
public interface TimestampColumnStats extends ColumnStats
{
    long getMinimum();

    long getMaximum();
}
