/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.View;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsMetadataProxy;

import javax.inject.Inject;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author: tao
 * @author hank
 * @date: Create in 2018-01-19 14:16
 **/
public class PixelsMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(PixelsMetadata.class);
    private final String connectorId;

    private final PixelsMetadataProxy pixelsMetadataProxy;

    @Inject
    public PixelsMetadata(PixelsConnectorId connectorId, PixelsMetadataProxy pixelsMetadataProxy)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pixelsMetadataProxy = requireNonNull(pixelsMetadataProxy, "pixelsMetadataProxy is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        try
        {
            return this.pixelsMetadataProxy.existSchema(schemaName);
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
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
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
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
                PixelsTableHandle tableHandle = new PixelsTableHandle(
                        connectorId, tableName.getSchemaName(), tableName.getTableName(), "");
                return tableHandle;
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns)
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
        checkArgument(tableHandle.getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");
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
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        List<ColumnMetadata> columns = columnHandleList.stream().map(PixelsColumnHandle::getColumnMetadata)
                .collect(toList());
        return new ConnectorTableMetadata(new SchemaTableName(schemaName, tableName), columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        try
        {
            List<String> schemaNames;
            if (schemaName.isPresent())
            {
                schemaNames = ImmutableList.of(schemaName.get());
            } else
            {
                schemaNames = pixelsMetadataProxy.getSchemaNames();
            }

            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (String schema : schemaNames)
            {
                /**
                 * Issue #179:
                 * Only try to get table names if the schema exists.
                 * 'show tables' in information_schema also invokes this method.
                 * In this case, information_schema does not exist in the metadata,
                 * we should return an empty list without throwing an exception.
                 * Presto will add the system tables by itself.
                 */
                if (pixelsMetadataProxy.existSchema(schema))
                {
                    for (String table : pixelsMetadataProxy.getTableNames(schema))
                    {
                        builder.add(new SchemaTableName(schema, table));
                    }
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        checkArgument(pixelsTableHandle.getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");

        List<PixelsColumnHandle> columnHandleList = null;
        try
        {
            columnHandleList = pixelsMetadataProxy.getTableColumn(
                    connectorId, pixelsTableHandle.getSchemaName(), pixelsTableHandle.getTableName());
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
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
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        if (prefix.getSchemaName() != null)
        {
            SchemaTableName tableName = new SchemaTableName(prefix.getSchemaName(), prefix.getTableName());
            try
            {
                /**
                 * Issue #183:
                 * Return an empty result if the table does not exist.
                 * This is possible when reading the content of information_schema tables.
                 */
                if (pixelsMetadataProxy.existTable(tableName.getSchemaName(), tableName.getTableName()))
                {
                    ConnectorTableMetadata tableMetadata = getTableMetadataInternal(
                            tableName.getSchemaName(), tableName.getTableName());
                    // table can disappear during listing operation
                    if (tableMetadata != null)
                    {
                        columns.put(tableName, tableMetadata.getColumns());
                    }
                }
            } catch (MetadataException e)
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle)
    {
        return ((PixelsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        String storageScheme = ((String) Optional.ofNullable(tableMetadata.getProperties().get("storage"))
                .orElse("hdfs")).toLowerCase(); // use HDFS by default.
        if (!Storage.Scheme.isValid(storageScheme))
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR,
                    "Unsupported storage scheme '" + storageScheme + "'.");
        }
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
            boolean res = this.pixelsMetadataProxy.createTable(schemaName, tableName, storageScheme, columns);
            if (res == false && ignoreExisting == false)
            {
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Table '" + schemaTableName + "' might already exist, failed to create it.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
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
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Table " + schemaName + "." + tableName + " does not exist.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
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
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Schema " + schemaName + " already exists.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
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
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Schema " + schemaName + " does not exist.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        try
        {
            boolean res = this.pixelsMetadataProxy.createView(viewName.getSchemaName(), viewName.getTableName(), viewData);
            if (res == false)
            {
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Failed to create view '" + viewName + "'.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try
        {
            boolean res = this.pixelsMetadataProxy.dropView(viewName.getSchemaName(), viewName.getTableName());
            if (res == false)
            {
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "View '" + viewName.getSchemaName() + "." + viewName.getTableName() + "' does not exist.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        try
        {
            List<String> schemaNames;
            if (schemaName.isPresent())
            {
                schemaNames = ImmutableList.of(schemaName.get());
            } else
            {
                schemaNames = pixelsMetadataProxy.getSchemaNames();
            }

            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (String schema : schemaNames)
            {
                /**
                 * Issue #194:
                 * Only try to get view names if the schema exists.
                 * 'show tables' in information_schema also invokes this method.
                 * In this case, information_schema does not exist in the metadata,
                 * we should return an empty list without throwing an exception.
                 * Presto will add the system tables by itself.
                 */
                if (pixelsMetadataProxy.existSchema(schema))
                {
                    for (String table : pixelsMetadataProxy.getViewNames(schema))
                    {
                        builder.add(new SchemaTableName(schema, table));
                    }
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> builder = ImmutableMap.builder();
        try
        {
            String schemaName = prefix.getSchemaName();
            if (this.pixelsMetadataProxy.existSchema(schemaName))
            {
                /**
                 * Issue #194:
                 * Only try to get views if the schema exists.
                 * Otherwise, return an empty set, which is required by Presto
                 * when reading the content of information_schema tables.
                 */
                List<View> views = this.pixelsMetadataProxy.getViews(schemaName);
                for (View view : views)
                {
                    SchemaTableName stName = new SchemaTableName(schemaName, view.getName());
                    builder.put(stName, new ConnectorViewDefinition(stName, Optional.empty(), view.getData()));
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }
}
