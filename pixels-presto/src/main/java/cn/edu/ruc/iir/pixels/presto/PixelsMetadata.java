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

import cn.edu.ruc.iir.pixels.presto.impl.PixelsMetadataReader;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto
 * @ClassName: PixelsMetadata
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-19 14:16
 **/
public class PixelsMetadata
        implements ConnectorMetadata {
    private final String connectorId;

    private final PixelsMetadataReader pixelsMetadataReader;

    private final Logger logger = Logger.get(PixelsMetadata.class);

    @Inject
    public PixelsMetadata(PixelsConnectorId connectorId, PixelsMetadataReader pixelsMetadataReader) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pixelsMetadataReader = requireNonNull(pixelsMetadataReader, "pixelsMetadataReader is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return listSchemaNamesInternal();
    }

    public List<String> listSchemaNamesInternal() {
        List<String> schemaNameList = pixelsMetadataReader.getSchemaNames();
        return schemaNameList;
    }

    @Override
    public PixelsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        requireNonNull(tableName, "tableName is null");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        PixelsTable table = pixelsMetadataReader.getTable(connectorId, tableName.getSchemaName(), tableName.getTableName());
        return table.getTableHandle();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        SchemaTableName tableName = tableHandle.toSchemaTableName();

        // create PixelsTableLayoutHandle
        PixelsTableLayoutHandle tableLayout = PixelsMetadataReader.getTableLayout(connectorId, tableName.getSchemaName(), tableName.getTableName());

        ConnectorTableLayout layout = getTableLayout(session, tableLayout);
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        checkArgument(tableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName());
        return getTableMetadata(tableName);
    }

    public ConnectorTableMetadata getTableMetadata(SchemaTableName tableName) {
        PixelsTable table = PixelsMetadataReader.getTable(connectorId, tableName.getSchemaName(), tableName.getTableName());
        List<ColumnMetadata> columns = table.getColumns().stream().map(PixelsColumnHandle::getColumnMetadata)
                .collect(toList());
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        List<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableList.of(schemaNameOrNull);
        } else {
            schemaNames = pixelsMetadataReader.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : pixelsMetadataReader.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        checkArgument(pixelsTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        PixelsTable table = pixelsMetadataReader.getTable(connectorId, pixelsTableHandle.getSchemaName(), pixelsTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(pixelsTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (PixelsColumnHandle column : table.getColumns()) {
            columnHandles.put(column.getColumnName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }


    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((PixelsColumnHandle) columnHandle).getColumnMetadata();
    }
}
