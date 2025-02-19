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
package io.pixelsdb.pixels.index.rocksdb;

import io.pixelsdb.pixels.common.index.SecondaryIndex;
import io.pixelsdb.pixels.common.index.SecondaryIndexProvider;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * @author hank
 * @create 2025-02-19
 */
public class RocksDBIndexProvider implements SecondaryIndexProvider
{
    // TODO: implement

    @Override
    public SecondaryIndex createInstance(@Nonnull SecondaryIndex.Scheme scheme) throws IOException
    {
        return null;
    }

    @Override
    public boolean compatibleWith(@Nonnull SecondaryIndex.Scheme scheme)
    {
        return false;
    }
}
