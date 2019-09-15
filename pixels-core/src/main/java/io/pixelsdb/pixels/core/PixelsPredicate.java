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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.core.stats.ColumnStats;

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
