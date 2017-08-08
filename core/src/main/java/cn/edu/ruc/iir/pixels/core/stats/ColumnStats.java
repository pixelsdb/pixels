package cn.edu.ruc.iir.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong
 */
public interface ColumnStats
{
    long getNumberOfValues();

    boolean hasNull();
}
