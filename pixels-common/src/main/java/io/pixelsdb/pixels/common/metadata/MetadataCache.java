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
 * @create 2022-07-13
 */
public class MetadataCache
{
    private static final MetadataCache instance = new MetadataCache();

    public static MetadataCache Instance()
    {
        return instance;
    }

    private class TransMetadata
    {
        private final Map<SchemaTableName, List<Column>> tableColumnsMap = new HashMap<>();
        private final Map<SchemaTableName, Table> tableMap = new HashMap<>();
    }

    private final Map<Long, TransMetadata> transMetadataMap = new HashMap<>();

    private MetadataCache() { }

    /**
     * Initialize metadata cache for a transaction. This method is idempotent.
     * For the same transaction id, before commit, calling this method after the first time is no-op.
     * @param transId the transaction id
     */
    public synchronized void initCache(long transId)
    {
        if (!this.transMetadataMap.containsKey(transId))
        {
            this.transMetadataMap.put(transId, new TransMetadata());
        }
    }

    /**
     * Drop the metadata cache of a transaction. This method should be only called after the transaction is commit.
     * @param transId the transaction id
     */
    public synchronized void dropCache(long transId)
    {
        this.transMetadataMap.remove(transId);
    }

    /**
     * @param transId the transaction id
     * @param schemaTableName the schema and table name of the table
     * @return true if the table is cached by the transaction
     */
    public synchronized boolean isTableCached(long transId, SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        TransMetadata transMetadata = this.transMetadataMap.get(transId);
        requireNonNull(transMetadata, "metadata cache is not initialized for the transaction");
        return transMetadata.tableMap.containsKey(schemaTableName);
    }

    /**
     * @param transId the transaction id
     * @param schemaTableName the schema and table name of the table
     * @return true if the column of the table is cached by the transaction
     */
    public synchronized boolean isTableColumnsCached(long transId, SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        TransMetadata transMetadata = this.transMetadataMap.get(transId);
        requireNonNull(transMetadata, "metadata cache is not initialized for the transaction");
        return transMetadata.tableColumnsMap.containsKey(schemaTableName);
    }

    /**
     * Cache the table metadata, refresh the cache if the table already exists.
     * @param transId the transaction id
     * @param schemaTableName the schema and table name of the table
     * @param table the metadata of the table
     */
    public synchronized void cacheTable(long transId, SchemaTableName schemaTableName, Table table)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(table, "table is null");
        TransMetadata transMetadata = this.transMetadataMap.get(transId);
        requireNonNull(transMetadata, "metadata cache is not initialized for the transaction");
        transMetadata.tableMap.put(schemaTableName, table);
    }

    /**
     * Get the cached metadata for the table.
     * @param transId the transaction id
     * @param schemaTableName the schema and table name of the table
     * @return null if the metadata of the table is not cached
     */
    public synchronized Table getTable(long transId, SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        TransMetadata transMetadata = this.transMetadataMap.get(transId);
        requireNonNull(transMetadata, "metadata cache is not initialized for the transaction");
        return transMetadata.tableMap.get(schemaTableName);
    }

    /**
     * Cache the metadata of the table's columns, refresh the cache if table columns are already cached.
     * @param transId the transaction id
     * @param schemaTableName the schema and table name of the table
     * @param columns the metadata of the table's columns
     */
    public synchronized void cacheTableColumns(long transId, SchemaTableName schemaTableName, List<Column> columns)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(columns, "columns is null");
        TransMetadata transMetadata = this.transMetadataMap.get(transId);
        requireNonNull(transMetadata, "metadata cache is not initialized for the transaction");
        transMetadata.tableColumnsMap.put(schemaTableName, ImmutableList.copyOf(columns));
    }

    /**
     * Get the cached column metadata for the table.
     * @param transId the transaction id
     * @param schemaTableName the schema and table name of the table
     * @return null if the column metadata of the table is not cached
     */
    public synchronized List<Column> getTableColumns(long transId, SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        TransMetadata transMetadata = this.transMetadataMap.get(transId);
        requireNonNull(transMetadata, "metadata cache is not initialized for the transaction");
        return transMetadata.tableColumnsMap.get(schemaTableName);
    }
}
