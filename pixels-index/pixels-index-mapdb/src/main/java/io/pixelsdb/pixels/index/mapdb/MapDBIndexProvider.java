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
package io.pixelsdb.pixels.index.mapdb;

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndexProvider;

import javax.annotation.Nonnull;

/**
 * @author hank
 * @create 2025-11-24
 */
public class MapDBIndexProvider implements SinglePointIndexProvider
{
    @Override
    public SinglePointIndex createInstance(long tableId, long indexId, @Nonnull SinglePointIndex.Scheme scheme,
                                           boolean unique) throws SinglePointIndexException
    {
        if (scheme == SinglePointIndex.Scheme.mapdb)
        {
            return new MapDBIndex(tableId, indexId, unique);
        }
        throw new SinglePointIndexException("unsupported scheme: " + scheme);
    }

    @Override
    public boolean compatibleWith(@Nonnull SinglePointIndex.Scheme scheme)
    {
        return scheme == SinglePointIndex.Scheme.mapdb;
    }
}
