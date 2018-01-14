/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.presto;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static cn.edu.ruc.iir.pixels.presto.PixelsConnector.*;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static java.text.MessageFormat.format;
import static java.util.stream.Collectors.*;

public class PixelsMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final Map<String, PixelsTableHandle> tables = new ConcurrentHashMap<>();

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return tables.get(tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        return pixelsTableHandle.toTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull != null && !schemaNameOrNull.equals(SCHEMA_NAME)) {
            return ImmutableList.of();
        }
        return tables.values().stream()
                .map(PixelsTableHandle::toSchemaTableName)
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        return pixelsTableHandle.getColumnHandles().stream()
                .collect(toMap(PixelsColumnHandle::getName, column -> column));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        PixelsColumnHandle pixelsColumnHandle = (PixelsColumnHandle) columnHandle;
        return pixelsColumnHandle.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.toSchemaTableName()))
                .collect(toMap(PixelsTableHandle::toSchemaTableName, handle -> handle.toTableMetadata().getColumns()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        tables.remove(pixelsTableHandle.getTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        PixelsTableHandle oldTableHandle = (PixelsTableHandle) tableHandle;
        PixelsTableHandle newTableHandle = new PixelsTableHandle(
                oldTableHandle.getSchemaName(),
                newTableName.getTableName(),
                oldTableHandle.getColumnHandles(),
                oldTableHandle.getSplitCount(),
                oldTableHandle.getPagesPerSplit(),
                oldTableHandle.getRowsPerPage(),
                oldTableHandle.getFieldsLength(),
                oldTableHandle.getPageProcessingDelay());
        tables.remove(oldTableHandle.getTableName());
        tables.put(newTableName.getTableName(), newTableHandle);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of());
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession connectorSession, ConnectorTableMetadata tableMetadata)
    {
        List<String> distributeColumns = (List<String>) tableMetadata.getProperties().get(DISTRIBUTED_ON);
        if (distributeColumns.isEmpty()) {
            return Optional.empty();
        }

        Set<String> undefinedColumns = Sets.difference(
                ImmutableSet.copyOf(distributeColumns),
                tableMetadata.getColumns().stream()
                        .map(ColumnMetadata::getName)
                        .collect(toSet()));
        if (!undefinedColumns.isEmpty()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Distribute columns not defined on table: " + undefinedColumns);
        }

        return Optional.of(new ConnectorNewTableLayout(PixelsPartitioningHandle.INSTANCE, distributeColumns));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        int splitCount = (Integer) tableMetadata.getProperties().get(SPLIT_COUNT_PROPERTY);
        int pagesPerSplit = (Integer) tableMetadata.getProperties().get(PAGES_PER_SPLIT_PROPERTY);
        int rowsPerPage = (Integer) tableMetadata.getProperties().get(ROWS_PER_PAGE_PROPERTY);
        int fieldsLength = (Integer) tableMetadata.getProperties().get(FIELD_LENGTH_PROPERTY);

        if (splitCount < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, SPLIT_COUNT_PROPERTY + " property is negative");
        }
        if (pagesPerSplit < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, PAGES_PER_SPLIT_PROPERTY + " property is negative");
        }
        if (rowsPerPage < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, ROWS_PER_PAGE_PROPERTY + " property is negative");
        }

        if (((splitCount > 0) || (pagesPerSplit > 0) || (rowsPerPage > 0)) &&
                ((splitCount == 0) || (pagesPerSplit == 0) || (rowsPerPage == 0))) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("All properties [%s, %s, %s] must be set if any are set",
                    SPLIT_COUNT_PROPERTY, PAGES_PER_SPLIT_PROPERTY, ROWS_PER_PAGE_PROPERTY));
        }

        Duration pageProcessingDelay = (Duration) tableMetadata.getProperties().get(PAGE_PROCESSING_DELAY);

        PixelsTableHandle handle = new PixelsTableHandle(
                tableMetadata,
                splitCount,
                pagesPerSplit,
                rowsPerPage,
                fieldsLength,
                pageProcessingDelay);
        return new PixelsOutputTableHandle(handle, pageProcessingDelay);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        PixelsOutputTableHandle pixelsOutputTableHandle = (PixelsOutputTableHandle) tableHandle;
        PixelsTableHandle table = pixelsOutputTableHandle.getTable();
        tables.put(table.getTableName(), table);
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle handle = (PixelsTableHandle) tableHandle;
        return new PixelsInsertTableHandle(handle.getPageProcessingDelay());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        return Optional.empty();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        PixelsTableHandle pixelsHandle = (PixelsTableHandle) handle;
        PixelsTableLayoutHandle layoutHandle = new PixelsTableLayoutHandle(
                pixelsHandle.getSplitCount(),
                pixelsHandle.getPagesPerSplit(),
                pixelsHandle.getRowsPerPage(),
                pixelsHandle.getFieldsLength(),
                pixelsHandle.getPageProcessingDelay());
        return ImmutableList.of(new ConnectorTableLayoutResult(getTableLayout(session, layoutHandle), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }
}
