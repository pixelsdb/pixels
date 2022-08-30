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
package io.pixelsdb.pixels.executor.plan;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.executor.plan.Table.TableType.BASE;
import static io.pixelsdb.pixels.executor.plan.Table.TableType.JOINED;

/**
 * The optimizer for serverless query plan.
 *
 * @author hank
 * @date 6/20/22
 */
public class PlanOptimizer
{
    private static final Logger logger = LogManager.getLogger(PlanOptimizer.class);
    private static final PlanOptimizer instance = new PlanOptimizer();

    public static PlanOptimizer Instance()
    {
        return instance;
    }

    private final MetadataService metadataService;
    private final double broadcastThresholdBytes;
    private final double broadcastThresholdRows;
    private final double joinPartitionSizeBytes;
    private final double joinPartitionSizeRows;
    private final boolean selectivityEnabled;
    private final double aggrPartitionSizeRows;

    /**
     * schemaTableName -> (columnName -> column)
     */
    private final Map<SchemaTableName, Map<String, Column>> columnStatisticCache = new HashMap<>();

    private PlanOptimizer()
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        String host = configFactory.getProperty("metadata.server.host");
        int port = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        metadataService = new MetadataService(host, port);
        String thresholdMB = configFactory.getProperty("join.broadcast.threshold.mb");
        broadcastThresholdBytes = Long.parseLong(thresholdMB) * 1024L * 1024L;
        String thresholdRows = configFactory.getProperty("join.broadcast.threshold.rows");
        broadcastThresholdRows = Long.parseLong(thresholdRows);
        String joinPartitionSizeMB = configFactory.getProperty("join.partition.size.mb");
        this.joinPartitionSizeBytes = Long.parseLong(joinPartitionSizeMB) * 1024L * 1024L;
        String joinPartitionSizeRows = configFactory.getProperty("join.partition.size.rows");
        this.joinPartitionSizeRows = Long.parseLong(joinPartitionSizeRows);
        this.selectivityEnabled = Boolean.parseBoolean(
                configFactory.getProperty("executor.selectivity.enabled"));
        String aggrPartitionSizeRows = configFactory.getProperty("aggr.partition.size.rows");
        this.aggrPartitionSizeRows = Long.parseLong(aggrPartitionSizeRows);
    }

    public JoinAlgorithm getJoinAlgorithm(Table leftTable, Table rightTable, JoinEndian joinEndian)
            throws MetadataException, InvalidProtocolBufferException
    {
        // Table size is used to estimate the read cost, it should NOT be multiplied by the selectivity.
        double smallTableSize;
        long smallTableRows;
        if (joinEndian == JoinEndian.SMALL_LEFT)
        {
            double selectivity = getTableSelectivity(leftTable);
            logger.debug("selectivity on table '" + leftTable.getTableName() + "': " + selectivity);
            smallTableSize = getTableInputSize(leftTable);
            smallTableRows = (long) (getTableRowCount(leftTable) * selectivity);
        }
        else
        {
            double selectivity = getTableSelectivity(rightTable);
            logger.debug("selectivity on table '" + rightTable.getTableName() + "': " + selectivity);
            smallTableSize = getTableInputSize(rightTable);
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
        logger.debug("selectivity on table '" + leftTable.getTableName() + "': " + leftSelectivity);
        logger.debug("selectivity on table '" + rightTable.getTableName() + "': " + rightSelectivity);
        long leftTableRowCount = (long) (getTableRowCount(leftTable) * leftSelectivity);
        long rightTableRowCount = (long) (getTableRowCount(rightTable) * rightSelectivity);

        double leftTableSize = getTableInputSize(leftTable);
        double rightTableSize = getTableInputSize(rightTable);
        if ((leftTableSize < broadcastThresholdBytes && leftTableRowCount < broadcastThresholdRows) &&
                (rightTableSize < broadcastThresholdBytes && rightTableRowCount < broadcastThresholdRows))
        {
            // Either table can be broadcast, mark the table with fewer rows as the small table.
            if (leftTableRowCount < rightTableRowCount)
            {
                return JoinEndian.SMALL_LEFT;
            }
            else
            {
                return JoinEndian.LARGE_LEFT;
            }
        }
        else if (leftTableSize < broadcastThresholdBytes && leftTableRowCount < broadcastThresholdRows)
        {
            // Only the left table can be broadcast, mark it as the small table.
            return JoinEndian.SMALL_LEFT;
        }
        else if (rightTableSize < broadcastThresholdBytes && rightTableRowCount < broadcastThresholdRows)
        {
            // Only the left table can be broadcast, mark it as the small table.
            return JoinEndian.LARGE_LEFT;
        }
        else
        {
            // This join would be partitioned join, mark the table with fewer rows as the small table.
            if (leftTableRowCount < rightTableRowCount)
            {
                return JoinEndian.SMALL_LEFT;
            } else
            {
                return JoinEndian.LARGE_LEFT;
            }
        }
    }

    public int getJoinNumPartition(Table leftTable, Table rightTable, JoinEndian joinEndian)
            throws MetadataException, InvalidProtocolBufferException
    {
        double totalSize = 0;
        double leftSelectivity = getTableSelectivity(leftTable);
        double rightSelectivity = getTableSelectivity(rightTable);
        logger.debug("selectivity on table '" + leftTable.getTableName() + "': " + leftSelectivity);
        logger.debug("selectivity on table '" + rightTable.getTableName() + "': " + rightSelectivity);
        totalSize += getTableInputSize(leftTable) * leftSelectivity;
        totalSize += getTableInputSize(rightTable) * rightSelectivity;
        double smallTableRowCount;
        double largeTableRowCount;
        if (joinEndian == JoinEndian.SMALL_LEFT)
        {
            smallTableRowCount = getTableRowCount(leftTable) * leftSelectivity;
            largeTableRowCount = getTableRowCount(rightTable) * rightSelectivity;
        }
        else
        {
            smallTableRowCount = getTableRowCount(rightTable) * rightSelectivity;
            largeTableRowCount = getTableRowCount(leftTable) * leftSelectivity;
        }

        int numFromSize = (int) Math.ceil(totalSize / joinPartitionSizeBytes);
        int numFromRows = (int) Math.ceil((smallTableRowCount + 0.1 * largeTableRowCount) / joinPartitionSizeRows);
        // Limit the partition size by choosing the maximum number of partitions.
        // TODO: estimate the join selectivity more accurately using histogram.
        return Math.max(Math.max(numFromSize, numFromRows), 8);
    }

    public int getAggrNumPartitions(AggregatedTable table) throws MetadataException
    {
        Table originTable = table.getAggregation().getOriginTable();
        int[] groupKeyColumnIds = table.getAggregation().getGroupKeyColumnIds();
        long cardinality = getTableCardinality(originTable, groupKeyColumnIds);
        int numPartitions = (int) (cardinality / aggrPartitionSizeRows);
        if (cardinality % aggrPartitionSizeRows > 0)
        {
            numPartitions++;
        }
        return numPartitions;
    }

    private long getTableCardinality(Table table, int[] keyColumnIds) throws MetadataException
    {
        if (table.getTableType() == BASE)
        {
            Map<String, Column> columnMap = getColumnMap((BaseTable) table);
            long cardinality = 0;
            for (int columnId : keyColumnIds)
            {
                String columnName = table.getColumnNames()[columnId];
                if (columnMap.get(columnName).getCardinality() > cardinality)
                {
                    cardinality = columnMap.get(columnName).getCardinality();
                }
            }
            return cardinality;
        }
        else if (table.getTableType() == JOINED)
        {
            JoinedTable joinedTable = (JoinedTable) table;
            Join join = joinedTable.getJoin();
            long cardinality = 0;
            for (int columnId : keyColumnIds)
            {
                String columnName = joinedTable.getColumnNames()[columnId];
                List<Integer> leftKeyColumnIds = new LinkedList<>();
                List<Integer> rightKeyColumnIds = new LinkedList<>();
                if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    if (columnId < join.getLeftColumnAlias().length)
                    {
                        checkArgument(join.getLeftColumnAlias()[columnId].equals(columnName),
                                "wrong column alias id for left table");
                        int leftColumnId = getColumnIdFromAliasId(join.getLeftProjection(), columnId);
                        leftKeyColumnIds.add(leftColumnId);
                    }
                    else
                    {
                        int rightAliasId = columnId - join.getLeftColumnAlias().length;
                        checkArgument(join.getRightColumnAlias()[rightAliasId].equals(columnName),
                                "wrong column alias id for right table");
                        int rightColumnId = getColumnIdFromAliasId(join.getRightProjection(), rightAliasId);
                        rightKeyColumnIds.add(rightColumnId);
                    }
                }
                else
                {
                    if (columnId < join.getRightColumnAlias().length)
                    {
                        checkArgument(join.getRightColumnAlias()[columnId].equals(columnName),
                                "wrong column alias id for right table");
                        int rightColumnId = getColumnIdFromAliasId(join.getRightProjection(), columnId);
                        rightKeyColumnIds.add(rightColumnId);
                    }
                    else
                    {
                        int leftAliasId = columnId - join.getRightColumnAlias().length;
                        checkArgument(join.getLeftColumnAlias()[leftAliasId].equals(columnName),
                                "wrong column alias id for left table");
                        int leftColumnId = getColumnIdFromAliasId(join.getLeftProjection(), leftAliasId);
                        leftKeyColumnIds.add(leftColumnId);
                    }
                }
                if (!leftKeyColumnIds.isEmpty())
                {
                    long columnCard = getTableCardinality(join.getLeftTable(),
                            leftKeyColumnIds.stream().mapToInt(Integer::intValue).toArray());
                    if (columnCard > cardinality)
                    {
                        cardinality = columnCard;
                    }
                }
                if (!rightKeyColumnIds.isEmpty())
                {
                    long columnCard = getTableCardinality(join.getRightTable(),
                            rightKeyColumnIds.stream().mapToInt(Integer::intValue).toArray());
                    if (columnCard > cardinality)
                    {
                        cardinality = columnCard;
                    }
                }
            }
            return cardinality;
        }
        else
        {
            throw new UnsupportedOperationException("originTable type " + table.getTableType() + " is not supported.");
        }
    }

    private int getColumnIdFromAliasId(boolean[] projection, int aliasId)
    {
        for (int i = 0, j = 0; i < projection.length; ++i)
        {
            if (j == aliasId)
            {
                return i;
            }
            if (projection[i])
            {
                j++;
            }
        }
        return -1;
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

    public double getTableSelectivity(Table table) throws MetadataException, InvalidProtocolBufferException
    {
        if (!this.selectivityEnabled)
        {
            return 1.0;
        }

        if (table.getTableType() == BASE)
        {
            Map<String, Column> columnMap = getColumnMap((BaseTable) table);
            double selectivity = 1.0;
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
                    if (columnStats.hasDateStatistics())
                    {
                        PixelsProto.DateStatistic dateStatistic = columnStats.getDateStatistics();
                        logger.debug("column " + column.getName() + " min: " + dateStatistic.getMinimum() + ", max: " + dateStatistic.getMaximum()
                        + ", selectivity: " + s + ", columnFilter: " + JSON.toJSONString(columnFilter));
                    }
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
