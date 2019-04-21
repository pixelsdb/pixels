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

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsMetadataProxy;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;
import java.util.*;

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_METASTORE_ERROR;
import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author: tao
 * @date: Create in 2018-01-19 14:16
 **/
public class PixelsMetadata
        implements ConnectorMetadata
{
    private static Logger logger = Logger.get(PixelsMetadata.class);
    private final String connectorId;

    private final PixelsMetadataProxy pixelsMetadataProxy;

    @Inject
    public PixelsMetadata(PixelsConnectorId connectorId, PixelsMetadataProxy pixelsMetadataProxy)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pixelsMetadataProxy = requireNonNull(pixelsMetadataProxy, "pixelsMetadataProxy is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNamesInternal();
    }

    private List<String> listSchemaNamesInternal()
    {
        List<String> schemaNameList = null;
        try
        {
            schemaNameList = pixelsMetadataProxy.getSchemaNames();
        } catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }
        return schemaNameList;
    }

    @Override
    public PixelsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        try
        {
            if (this.pixelsMetadataProxy.existTable(tableName.getSchemaName(), tableName.getTableName()))
            {
                PixelsTableHandle tableHandle = new PixelsTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName(), "");
                return tableHandle;
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }
        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        PixelsTableLayoutHandle tableLayout = new PixelsTableLayoutHandle(tableHandle);
        tableLayout.setConstraint(constraint.getSummary());
        if(desiredColumns.isPresent())
            tableLayout.setDesiredColumns(desiredColumns.get());
        ConnectorTableLayout layout = getTableLayout(session, tableLayout);
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        checkArgument(tableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        return getTableMetadataInternal(tableHandle.getSchemaName(), tableHandle.getTableName());
    }

    private ConnectorTableMetadata getTableMetadataInternal(String schemaName, String tableName)
    {
        List<PixelsColumnHandle> columnHandleList;
        try
        {
            columnHandleList = pixelsMetadataProxy.getTableColumn(connectorId, schemaName, tableName);
        } catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }
        List<ColumnMetadata> columns = columnHandleList.stream().map(PixelsColumnHandle::getColumnMetadata)
                .collect(toList());
        return new ConnectorTableMetadata(new SchemaTableName(schemaName, tableName), columns);
    }

    private ConnectorTableMetadata getTableMetadataInternal(SchemaTableName schemaTableName)
    {
        return getTableMetadataInternal(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        try
        {
            List<String> schemaNames;
            if (schemaNameOrNull != null)
            {
                schemaNames = ImmutableList.of(schemaNameOrNull);
            } else
            {
                schemaNames = pixelsMetadataProxy.getSchemaNames();
            }

            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (String schemaName : schemaNames)
            {
                for (String tableName : pixelsMetadataProxy.getTableNames(schemaName))
                {
                    builder.add(new SchemaTableName(schemaName, tableName));
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        checkArgument(pixelsTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        List<PixelsColumnHandle> columnHandleList = null;
        try
        {
            columnHandleList = pixelsMetadataProxy.getTableColumn(connectorId, pixelsTableHandle.getSchemaName(), pixelsTableHandle.getTableName());
        } catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }
        if (columnHandleList == null)
        {
            throw new TableNotFoundException(pixelsTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (PixelsColumnHandle column : columnHandleList)
        {
            columnHandles.put(column.getColumnName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTablesInternal(session, prefix))
        {
            ConnectorTableMetadata tableMetadata = getTableMetadataInternal(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null)
            {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTablesInternal(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null)
        {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((PixelsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<Column> columns = new ArrayList<>();
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns())
        {
            Column column = new Column();
            column.setName(columnMetadata.getName());
            // columnMetadata.getType().getDisplayName(); is the same as
            // columnMetadata.getType().getTypeSignature().toString();
            column.setType(columnMetadata.getType().getDisplayName());
            // column size is set to 0 when the table is just created.
            column.setSize(0);
            columns.add(column);
        }
        try
        {
            logger.debug("create table: column number=" + columns.size());
            boolean res = this.pixelsMetadataProxy.createTable(schemaName, tableName, columns);
            if (res == false && ignoreExisting == false)
            {
                throw  new PrestoException(PIXELS_SQL_EXECUTE_ERROR, "table " + schemaTableName.toString() +
                " exists");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }
    }

    /**
     * Begin the atomic creation of a table with data.
     *
     * @param session
     * @param tableMetadata
     * @param layout
     */
    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        throw  new PrestoException(PIXELS_SQL_EXECUTE_ERROR, "create table with data is currently not supported.");
    }

    /**
     * Finish a table creation with data after the data is written.
     *
     * @param session
     * @param tableHandle
     * @param fragments
     */
    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw  new PrestoException(PIXELS_SQL_EXECUTE_ERROR, "create table with data is currently not supported.");
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        String schemaName = pixelsTableHandle.getSchemaName();
        String tableName = pixelsTableHandle.getTableName();

        try
        {
            boolean res = this.pixelsMetadataProxy.dropTable(schemaName, tableName);
            if (res == false)
            {
                throw  new PrestoException(PIXELS_SQL_EXECUTE_ERROR, "no such table " + schemaName +
                        "." + tableName);
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        try
        {
            boolean res = this.pixelsMetadataProxy.createSchema(schemaName);
            if (res == false)
            {
                throw  new PrestoException(PIXELS_SQL_EXECUTE_ERROR, "schema " + schemaName +
                        " exists");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        try
        {
            boolean res = this.pixelsMetadataProxy.dropSchema(schemaName);
            if (res == false)
            {
                throw  new PrestoException(PIXELS_SQL_EXECUTE_ERROR, "no such schema " + schemaName);
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }
    }
}
