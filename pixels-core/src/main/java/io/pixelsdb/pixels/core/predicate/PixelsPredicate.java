/*
 * Copyright 2017-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.predicate;

import io.pixelsdb.pixels.core.stats.ColumnStats;

import java.util.Map;

/**
 * pixels
 *
 * @author guodong
 */
public interface PixelsPredicate
{
    PixelsPredicate TRUE_PREDICATE = new PixelsPredicate()
    {
        @Override
        public boolean matches(long numberOfRows, Map<Integer, ColumnStats> statisticsByColumnIndex)
        {
            return true;
        }

        @Override
        public boolean matchesNone()
        {
            return false;
        }

        @Override
        public boolean matchesAll()
        {
            return true;
        }
    };

    PixelsPredicate FALSE_PREDICATE = new PixelsPredicate()
    {
        @Override
        public boolean matches(long numberOfRows, Map<Integer, ColumnStats> statisticsByColumnIndex)
        {
            return false;
        }

        @Override
        public boolean matchesNone()
        {
            return true;
        }

        @Override
        public boolean matchesAll()
        {
            return false;
        }
    };

    /**
     * Check if predicate matches column statistics.
     * Note that on the same column, onlyNull (e.g. 'is null') predicate will match hasNull statistics
     * and vice versa.
     *
     * TODO: pay attention to the correctness of this method.
     *
     * @param numberOfRows            number of rows in the corresponding horizontal data unit
     *                                (pixel, row group, file, etc.) where the statistics come from.
     * @param statisticsByColumnIndex statistics map. key: column index in user specified schema,
     *                                value: column statistic
     */
    boolean matches(long numberOfRows, Map<Integer, ColumnStats> statisticsByColumnIndex);

    /**
     * Added in Issue #103.
     * This method relies on TupleDomain.isNone() in presto spi,
     * which is mysterious.
     * TODO: pay attention to the correctness of this method.
     * @return true if this predicate will never match any values.
     */
    boolean matchesNone();

    /**
     * Added in Issue #103.
     * This method relies on TupleDomain.isNone() in presto spi,
     * which is mysterious.
     * TODO: pay attention to the correctness of this method.
     * @return true if this predicate will match any values.
     */
    boolean matchesAll();
}
