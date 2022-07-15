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
package io.pixelsdb.pixels.executor.join;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.plan.JoinEndian;
import io.pixelsdb.pixels.executor.plan.JoinedTable;
import io.pixelsdb.pixels.executor.plan.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.executor.plan.Table.TableType.BASE;
import static io.pixelsdb.pixels.executor.plan.Table.TableType.JOINED;

/**
 * The advisor for serverless joins.
 *
 * @author hank
 * @date 6/20/22
 */
public class JoinAdvisor
{
    private static final JoinAdvisor instance = new JoinAdvisor();

    public static JoinAdvisor Instance()
    {
        return instance;
    }

    private final MetadataService metadataService;
    private final double broadcastThresholdMB;
    private final double broadcastThresholdRows;
    private final double partitionSize;
    /**
     * schemaTableName -> (columnName -> column)
     */
    private final Map<SchemaTableName, Map<String, Column>> columnStatisticCache = new HashMap<>();

    private JoinAdvisor()
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        String host = configFactory.getProperty("metadata.server.host");
        int port = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        metadataService = new MetadataService(host, port);
        String thresholdMB = configFactory.getProperty("join.broadcast.threshold.mb");
        broadcastThresholdMB = Integer.parseInt(thresholdMB) * 1024 * 1024;
        String thresholdRows = configFactory.getProperty("join.broadcast.threshold.rows");
        broadcastThresholdRows = Integer.parseInt(thresholdRows);
        String partitionSizeMB = configFactory.getProperty("join.partition.size.mb");
        partitionSize = Integer.parseInt(partitionSizeMB) * 1024 * 1024;
    }

    public JoinAlgorithm getJoinAlgorithm(Table leftTable, Table rightTable, JoinEndian joinEndian)
            throws MetadataException
    {
        double smallTableSize;
        long smallTableRows;
        if (joinEndian == JoinEndian.SMALL_LEFT)
        {
            smallTableSize = getTableInputSize(leftTable);
            smallTableRows = getTableRowCount(leftTable);
        }
        else
        {
            smallTableSize = getTableInputSize(rightTable);
            smallTableRows = getTableRowCount(rightTable);
        }
        if (smallTableSize >= broadcastThresholdMB || smallTableRows > broadcastThresholdRows)
        {
            return JoinAlgorithm.PARTITIONED;
        }
        else
        {
            return JoinAlgorithm.BROADCAST;
        }
        // Chain joins are only generated during execution.
    }

    public JoinEndian getJoinEndian(Table leftTable, Table rightTable) throws MetadataException
    {
        double leftTableSize = getTableInputSize(leftTable);
        double rightTableSize = getTableInputSize(rightTable);
        if (leftTableSize < rightTableSize)
        {
            return JoinEndian.SMALL_LEFT;
        }
        else
        {
            return JoinEndian.LARGE_LEFT;
        }
    }

    public int getNumPartition(Table leftTable, Table rightTable, JoinEndian joinEndian)
            throws MetadataException
    {
        double largeTableSize;
        if (joinEndian == JoinEndian.LARGE_LEFT)
        {
            largeTableSize = getTableInputSize(leftTable);
        }
        else
        {
            largeTableSize = getTableInputSize(rightTable);
        }
        return (int) Math.ceil(largeTableSize / partitionSize);
    }

    private double getTableInputSize(Table table) throws MetadataException
    {
        if (table.getTableType() == BASE)
        {
            SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
            Map<String, Column> columnMap;
            if (columnStatisticCache.containsKey(schemaTableName))
            {
                columnMap = columnStatisticCache.get(schemaTableName);
            }
            else
            {
                List<Column> columns = MetadataCache.Instance().getTableColumns(schemaTableName);
                if (columns == null)
                {
                    columns = metadataService.getColumns(
                            table.getSchemaName(), table.getTableName(), true);
                    MetadataCache.Instance().cacheTableColumns(schemaTableName, columns);
                }
                columnMap = new HashMap<>(columns.size());
                for (Column column : columns)
                {
                    columnMap.put(column.getName(), column);
                }
                columnStatisticCache.put(schemaTableName, columnMap);
            }
            double size = 0;
            for (String columnName : table.getColumnNames())
            {
                size += columnMap.get(columnName).getSize();
            }
            return size;
        }
        checkArgument(table.getTableType() == JOINED, "the table is not a base or joined table");
        JoinedTable joinedTable = (JoinedTable) table;
        Table leftTable = joinedTable.getJoin().getLeftTable();
        Table rightTable = joinedTable.getJoin().getRightTable();
        // We estimate the size of the joined table by the sum of the two tables' size.
        return getTableInputSize(leftTable) + getTableInputSize(rightTable);
    }

    protected long getTableRowCount(Table table) throws MetadataException
    {
        if (table.getTableType() == BASE)
        {
            SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
            io.pixelsdb.pixels.common.metadata.domain.Table metadataTable =
                    MetadataCache.Instance().getTable(schemaTableName);
            if (metadataTable == null)
            {
                metadataTable = metadataService.getTable(
                        table.getSchemaName(), table.getTableName());
                MetadataCache.Instance().cacheTable(schemaTableName, metadataTable);
            }
            return metadataTable.getRowCount();
        }
        checkArgument(table.getTableType() == JOINED, "the table is not a base or joined table");
        JoinedTable joinedTable = (JoinedTable) table;
        Table leftTable = joinedTable.getJoin().getLeftTable();
        Table rightTable = joinedTable.getJoin().getRightTable();
        // We estimate the number of rows in the joined table by the max of the two tables' row count.
        return Math.max(getTableRowCount(leftTable), getTableRowCount(rightTable));
    }
}
