package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.stats.ColumnStats;

import java.util.Map;

/**
 * pixels
 *
 * @author guodong
 */
public interface PixelsPredicate
{
    PixelsPredicate TRUE_PREDICATE = (numberOfRows, statisticsByColumnIndex) -> true;

    PixelsPredicate FALSE_PREDICATE = ((numberOfRows, statisticsByColumnIndex) -> false);

    /**
     * Check if predicate matches statistics
     *
     * @param numberOfRows            number of rows
     * @param statisticsByColumnIndex statistics map. key: column index in user specified schema,
     *                                value: column statistic
     */
    boolean matches(long numberOfRows, Map<Integer, ColumnStats> statisticsByColumnIndex);
}
