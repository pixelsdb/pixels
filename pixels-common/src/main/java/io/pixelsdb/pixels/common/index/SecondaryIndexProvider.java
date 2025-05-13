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

import javax.annotation.Nonnull;
import java.io.IOException;
import org.rocksdb.RocksDBException;

/**
 * @author hank
 * @create 2025-02-08
 */
public interface SecondaryIndexProvider
{
    /**
     * Create an instance of the secondary index.
     */
    SecondaryIndex createInstance(@Nonnull SecondaryIndex.Scheme scheme) throws IOException;

    /**
     * @param scheme the given secondary index scheme.
     * @return true if this secondary index provider is compatible with the given scheme.
     */
    boolean compatibleWith(@Nonnull SecondaryIndex.Scheme scheme);
}
