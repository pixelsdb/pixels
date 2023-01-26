/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.common.metadata;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 13/07/2022
 */
public class MetadataCache
{
    private static final MetadataCache instance = new MetadataCache();

    public static MetadataCache Instance()
    {
        return instance;
    }

    private final Map<SchemaTableName, List<Column>> tableColumnsMap = new HashMap<>();

    private final Map<SchemaTableName, Table> tableMap = new HashMap<>();

    private MetadataCache() { }

    public void cacheTable(SchemaTableName schemaTableName, Table table)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(table, "table is null");
        this.tableMap.put(schemaTableName, table);
    }

    public Table getTable(SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        return this.tableMap.get(schemaTableName);
    }

    public synchronized void cacheTableColumns(SchemaTableName schemaTableName, List<Column> columns)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(columns, "columns is null");
        this.tableColumnsMap.put(schemaTableName, ImmutableList.copyOf(columns));
    }

    /**
     * Get the cached column metadata for the table.
     * @param schemaTableName the schema and table name of the table
     * @return null for cache miss
     */
    public synchronized List<Column> getTableColumns(SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        return this.tableColumnsMap.get(schemaTableName);
    }

    public synchronized void dropCachedColumns()
    {
        this.tableColumnsMap.clear();
    }
}
