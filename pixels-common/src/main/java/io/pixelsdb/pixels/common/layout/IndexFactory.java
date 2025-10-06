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

import io.pixelsdb.pixels.common.metadata.SchemaTableName;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IndexFactory
{
    private static final class InstanceHolder
    {
        private static final IndexFactory instance = new IndexFactory();
    }

    public static IndexFactory Instance()
    {
        return InstanceHolder.instance;
    }

    private final Map<SchemaTableName, SplitsIndex> splitsIndexes;
    private final Map<SchemaTableName, ProjectionsIndex> projectionsIndexes;

    private IndexFactory()
    {
        this.splitsIndexes = new ConcurrentHashMap<>();
        this.projectionsIndexes = new ConcurrentHashMap<>();
    }

    public void cacheSplitsIndex(SchemaTableName entry, SplitsIndex splitsIndex)
    {
        this.splitsIndexes.put(entry, splitsIndex);
    }

    public SplitsIndex getSplitsIndex(SchemaTableName entry)
    {
        return this.splitsIndexes.get(entry);
    }

    public void dropSplitsIndex(SchemaTableName entry)
    {
        this.splitsIndexes.remove(entry);
    }

    public void dropSplitsIndex(String schemaName, String tableName)
    {
        this.dropSplitsIndex(new SchemaTableName(schemaName, tableName));
    }

    public void cacheProjectionsIndex(SchemaTableName entry, ProjectionsIndex projectionsIndex)
    {
        this.projectionsIndexes.put(entry, projectionsIndex);
    }

    public ProjectionsIndex getProjectionsIndex(SchemaTableName entry)
    {
        return this.projectionsIndexes.get(entry);
    }

    public void dropProjectionsIndex(SchemaTableName entry)
    {
        this.projectionsIndexes.remove(entry);
    }

    public void dropProjectionsIndex(String schemaName, String tableName)
    {
        this.dropProjectionsIndex(new SchemaTableName(schemaName, tableName));
    }
}
