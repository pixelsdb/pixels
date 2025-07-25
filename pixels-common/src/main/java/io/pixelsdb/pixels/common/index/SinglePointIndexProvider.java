/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;

import javax.annotation.Nonnull;

/**
 * @author hank
 * @create 2025-02-08
 */
public interface SinglePointIndexProvider
{
    /**
     * Create an instance of the single point index.
     */
    SinglePointIndex createInstance(long tableId, long indexId, @Nonnull SinglePointIndex.Scheme scheme, boolean unique)
            throws SinglePointIndexException;

    /**
     * @param scheme the given single point index scheme.
     * @return true if this single point index provider is compatible with the given scheme.
     */
    boolean compatibleWith(@Nonnull SinglePointIndex.Scheme scheme);
}
