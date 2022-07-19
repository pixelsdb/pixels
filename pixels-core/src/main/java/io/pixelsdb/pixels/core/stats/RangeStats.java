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
package io.pixelsdb.pixels.core.stats;

/**
 * pixels
 *
 * @author guodong, hank
 */
public interface RangeStats<T>
{
    T getMinimum();

    T getMaximum();

    /**
     * @return true if the minimum value is present.
     */
    boolean hasMinimum();

    /**
     * @return true if the maximum value is present.
     */
    boolean hasMaximum();

    /**
     * Get the estimation of selectivity given the selection range.
     * @param lowerBound the Pixels native value for the lower bound of the range, null if not lower bounded
     * @param lowerInclusive true if the lower bound is included
     * @param upperBound the Pixels native value for the upper bound of the range, null if not upper bounded
     * @param upperInclusive true if the upper bound is included
     * @return the estimated selectivity, for example 0.05 means 5%, negative if the selectivity can not be estimated
     */
    double getSelectivity(Object lowerBound, boolean lowerInclusive, Object upperBound, boolean upperInclusive);
}
