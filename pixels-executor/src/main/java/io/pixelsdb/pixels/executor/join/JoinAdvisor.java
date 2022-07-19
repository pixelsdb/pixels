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

import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.executor.plan.BaseTable;
import io.pixelsdb.pixels.executor.plan.JoinEndian;
import io.pixelsdb.pixels.executor.plan.JoinedTable;
import io.pixelsdb.pixels.executor.plan.Table;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
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
    private static final Logger logger = LogManager.getLogger(JoinAdvisor.class);
    private static final JoinAdvisor instance = new JoinAdvisor();

    public static JoinAdvisor Instance()
    {
        return instance;
    }

    private final MetadataService metadataService;
    private final double broadcastThresholdBytes;
    private final double broadcastThresholdRows;
    private final double partitionSizeBytes;
    private final double partitionSizeRows;
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
        broadcastThresholdBytes = Long.parseLong(thresholdMB) * 1024L * 1024L;
        String thresholdRows = configFactory.getProperty("join.broadcast.threshold.rows");
        broadcastThresholdRows = Long.parseLong(thresholdRows);
        String partitionSizeMB = configFactory.getProperty("join.partition.size.mb");
        this.partitionSizeBytes = Long.parseLong(partitionSizeMB) * 1024L * 1024L;
        String partitionSizeRows = configFactory.getProperty("join.partition.size.rows");
        this.partitionSizeRows = Long.parseLong(partitionSizeRows);
    }

    public JoinAlgorithm getJoinAlgorithm(Table leftTable, Table rightTable, JoinEndian joinEndian)
            throws MetadataException, InvalidProtocolBufferException
    {
        double smallTableSize;
        long smallTableRows;
        if (joinEndian == JoinEndian.SMALL_LEFT)
        {
            double selectivity = getTableSelectivity(leftTable);
            logger.info("selectivity on table '" + leftTable.getTableName() + "': " + selectivity);
            smallTableSize = getTableInputSize(leftTable) * selectivity;
            smallTableRows = (long) (getTableRowCount(leftTable) * selectivity);
        }
        else
        {
            double selectivity = getTableSelectivity(rightTable);
            logger.info("selectivity on table '" + rightTable.getTableName() + "': " + selectivity);
            smallTableSize = getTableInputSize(rightTable) * selectivity;
            smallTableRows = (long) (getTableRowCount(rightTable) * selectivity);
        }
        if (smallTableSize >= broadcastThresholdBytes || smallTableRows > broadcastThresholdRows)
        {
            return JoinAlgorithm.PARTITIONED;
        }
        else
        {
            return JoinAlgorithm.BROADCAST;
        }
        // Chain joins are only generated during execution.
    }

    public JoinEndian getJoinEndian(Table leftTable, Table rightTable) throws MetadataException, InvalidProtocolBufferException
    {
        // Use row count instead of input size, because building hash table is the main cost.
        double leftSelectivity = getTableSelectivity(leftTable);
        double rightSelectivity = getTableSelectivity(rightTable);
        logger.info("selectivity on table '" + leftTable.getTableName() + "': " + leftSelectivity);
        logger.info("selectivity on table '" + rightTable.getTableName() + "': " + rightSelectivity);
        long leftTableRowCount = (long) (getTableRowCount(leftTable) * leftSelectivity);
        long rightTableRowCount = (long) (getTableRowCount(rightTable) * rightSelectivity);
        if (leftTableRowCount < rightTableRowCount)
        {
            return JoinEndian.SMALL_LEFT;
        }
        else
        {
            return JoinEndian.LARGE_LEFT;
        }
    }

    public int getNumPartition(Table leftTable, Table rightTable, JoinEndian joinEndian)
            throws MetadataException, InvalidProtocolBufferException
    {
        double largeTableSize;
        double largeTableRowCount;
        if (joinEndian == JoinEndian.LARGE_LEFT)
        {
            largeTableSize = getTableInputSize(leftTable);
            largeTableRowCount = getTableRowCount(leftTable);
        }
        else
        {
            largeTableSize = getTableInputSize(rightTable);
            largeTableRowCount = getTableRowCount(rightTable);
        }
        double leftSelectivity = getTableSelectivity(leftTable);
        double rightSelectivity = getTableSelectivity(rightTable);
        logger.info("selectivity on table '" + leftTable.getTableName() + "': " + leftSelectivity);
        logger.info("selectivity on table '" + rightTable.getTableName() + "': " + rightSelectivity);
        double selectivity = Math.min(leftSelectivity, rightSelectivity);
        int numFromSize = (int) Math.ceil(selectivity * largeTableSize / partitionSizeBytes);
        int numFromRows = (int) Math.ceil(selectivity * largeTableRowCount / partitionSizeRows);
        // Limit the partition size by choosing the maximum number of partitions.
        return Math.max(numFromSize, numFromRows);
    }

    private double getTableInputSize(Table table) throws MetadataException
    {
        if (table.getTableType() == BASE)
        {
            Map<String, Column> columnMap = getColumnMap((BaseTable) table);
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

    protected double getTableSelectivity(Table table) throws MetadataException, InvalidProtocolBufferException
    {
        if (table.getTableType() == BASE)
        {
            Map<String, Column> columnMap = getColumnMap((BaseTable) table);
            double selectivity = 1;
            TableScanFilter tableScanFilter = ((BaseTable) table).getFilter();
            String[] columnsToRead = table.getColumnNames();
            for (int i = 0; i < columnsToRead.length; ++i)
            {
                ColumnFilter<?> columnFilter = tableScanFilter.getColumnFilter(i);
                if (columnFilter != null)
                {
                    Column column = columnMap.get(columnsToRead[i]);
                    ByteBuffer buffer = column.getRecordStats();
                    PixelsProto.ColumnStatistic columnStats = PixelsProto.ColumnStatistic.parseFrom(buffer);
                    double s = columnFilter.getSelectivity(column.getNullFraction(), column.getCardinality(), columnStats);
                    if (s >= 0 && s < selectivity)
                    {
                        // Use the minimum selectivity of the columns as the selectivity on this table.
                        selectivity = s;
                    }
                }
            }
            return selectivity;
        }
        checkArgument(table.getTableType() == JOINED, "the table is not a base or joined table");
        JoinedTable joinedTable = (JoinedTable) table;
        Table leftTable = joinedTable.getJoin().getLeftTable();
        Table rightTable = joinedTable.getJoin().getRightTable();
        // We estimate the selectivity the joined table by the minimum of the two tables' selectivity.
        return Math.min(getTableSelectivity(leftTable), getTableSelectivity(rightTable));
    }

    private Map<String, Column> getColumnMap(BaseTable table) throws MetadataException
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
        return columnMap;
    }
}
