/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.common.layout;

import java.util.HashMap;
import java.util.Map;

public class IndexFactory
{
    private static IndexFactory instance = null;

    public static IndexFactory Instance()
    {
        if (instance == null)
        {
            instance = new IndexFactory();
        }
        return instance;
    }

    private Map<IndexName, SplitsIndex> splitsIndexes = null;

    private IndexFactory()
    {
        this.splitsIndexes = new HashMap<>();
    }

    public void cacheSplitsIndex(IndexName entry, SplitsIndex splitsIndex)
    {
        this.splitsIndexes.put(entry, splitsIndex);
    }

    public SplitsIndex getSplitsIndex(IndexName entry)
    {
        return this.splitsIndexes.get(entry);
    }

}
