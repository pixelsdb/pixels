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
package io.pixelsdb.pixels.index.memory;

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndexProvider;

import javax.annotation.Nonnull;

/**
 * @author hank
 * @create 2025-10-11
 */
public class MemoryIndexProvider implements SinglePointIndexProvider
{
    @Override
    public SinglePointIndex createInstance(long tableId, long indexId, @Nonnull SinglePointIndex.Scheme scheme,
                                           boolean unique, IndexOption indexOption) throws SinglePointIndexException
    {
        if (scheme == SinglePointIndex.Scheme.memory)
        {
            return new MemoryIndex(tableId, indexId, unique);
        }
        throw new SinglePointIndexException("Unsupported scheme: " + scheme);
    }

    @Override
    public boolean compatibleWith(@Nonnull SinglePointIndex.Scheme scheme)
    {
        return scheme == SinglePointIndex.Scheme.memory;
    }
}
