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
package io.pixelsdb.pixels.index.main.sqlite;

import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import javax.annotation.Nonnull;

/**
 * @author hank
 * @create 2025-07-20
 */
public class SqliteMainIndexProvider implements MainIndexProvider
{
    private static final String sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");

    @Override
    public MainIndex createInstance(long tableId, @Nonnull MainIndex.Scheme scheme) throws MainIndexException
    {
        if (scheme == MainIndex.Scheme.sqlite)
        {
            try
            {
                return new SqliteMainIndex(tableId, sqlitePath);
            }
            catch (MainIndexException e)
            {
                throw new MainIndexException("failed to create SQLite instance", e);
            }
        }
        throw new MainIndexException("unsupported scheme: " + scheme);
    }

    @Override
    public boolean compatibleWith(@Nonnull MainIndex.Scheme scheme)
    {
        return scheme == MainIndex.Scheme.sqlite;
    }
}
