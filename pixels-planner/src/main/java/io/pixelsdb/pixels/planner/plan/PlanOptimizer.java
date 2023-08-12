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
package io.pixelsdb.pixels.planner.plan;

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
import io.pixelsdb.pixels.planner.plan.logical.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.planner.plan.logical.Table.TableType.BASE;
import static io.pixelsdb.pixels.planner.plan.logical.Table.TableType.JOINED;

/**
 * The optimizer for serverless query plan.
 *
 * @author hank
 * @create 2022-06-20
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

    public JoinAlgorithm getJoinAlgorithm(long transId, Table leftTable, Table rightTable, JoinEndian joinEndian)
            throws MetadataException, InvalidProtocolBufferException
    {
        // Table size is used to estimate the read cost, it should NOT be multiplied by the selectivity.
        double smallTableSize;
        long smallTableRows;
        if (joinEndian == JoinEndian.SMALL_LEFT)
        {
            double selectivity = getTableSelectivity(transId, leftTable);
            logger.debug("selectivity on table '" + leftTable.getTableName() + "': " + selectivity);
            smallTableSize = getTableInputSize(transId, leftTable);
            smallTableRows = (long) (getTableRowCount(transId, leftTable) * selectivity);
        }
        else
        {
            double selectivity = getTableSelectivity(transId, rightTable);
            logger.debug("selectivity on table '" + rightTable.getTableName() + "': " + selectivity);
            smallTableSize = getTableInputSize(transId, rightTable);
            smallTableRows = (long) (getTableRowCount(transId, rightTable) * selectivity);
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

    public JoinEndian getJoinEndian(long transId, Table leftTable, Table rightTable)
            throws MetadataException, InvalidProtocolBufferException
    {
        // Use row count instead of input size, because building hash table is the main cost.
        double leftSelectivity = getTableSelectivity(transId, leftTable);
        double rightSelectivity = getTableSelectivity(transId, rightTable);
        logger.debug("selectivity on table '" + leftTable.getTableName() + "': " + leftSelectivity);
        logger.debug("selectivity on table '" + rightTable.getTableName() + "': " + rightSelectivity);
        long leftTableRowCount = (long) (getTableRowCount(transId, leftTable) * leftSelectivity);
        long rightTableRowCount = (long) (getTableRowCount(transId, rightTable) * rightSelectivity);

        double leftTableSize = getTableInputSize(transId, leftTable);
        double rightTableSize = getTableInputSize(transId, rightTable);
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

    public int getJoinNumPartition(long transId, Table leftTable, Table rightTable, JoinEndian joinEndian)
            throws MetadataException, InvalidProtocolBufferException
    {
        double totalSize = 0;
        double leftSelectivity = getTableSelectivity(transId, leftTable);
        double rightSelectivity = getTableSelectivity(transId, rightTable);
        logger.debug("selectivity on table '" + leftTable.getTableName() + "': " + leftSelectivity);
        logger.debug("selectivity on table '" + rightTable.getTableName() + "': " + rightSelectivity);
        totalSize += getTableInputSize(transId, leftTable) * leftSelectivity;
        totalSize += getTableInputSize(transId, rightTable) * rightSelectivity;
        double smallTableRowCount;
        double largeTableRowCount;
        if (joinEndian == JoinEndian.SMALL_LEFT)
        {
            smallTableRowCount = getTableRowCount(transId, leftTable) * leftSelectivity;
            largeTableRowCount = getTableRowCount(transId, rightTable) * rightSelectivity;
        }
        else
        {
            smallTableRowCount = getTableRowCount(transId, rightTable) * rightSelectivity;
            largeTableRowCount = getTableRowCount(transId, leftTable) * leftSelectivity;
        }

        int numFromSize = (int) Math.ceil(totalSize / joinPartitionSizeBytes);
        int numFromRows = (int) Math.ceil((smallTableRowCount + 0.1 * largeTableRowCount) / joinPartitionSizeRows);
        // Limit the partition size by choosing the maximum number of partitions.
        // TODO: estimate the join selectivity more accurately using histogram.
        return Math.max(Math.max(numFromSize, numFromRows), 8);
    }

    public int getAggrNumPartitions(long transId, AggregatedTable table) throws MetadataException
    {
        Table originTable = table.getAggregation().getOriginTable();
        int[] groupKeyColumnIds = table.getAggregation().getGroupKeyColumnIds();
        long cardinality = getTableCardinality(transId, originTable, groupKeyColumnIds);
        int numPartitions = (int) (cardinality / aggrPartitionSizeRows);
        if (cardinality % aggrPartitionSizeRows > 0)
        {
            numPartitions++;
        }
        return numPartitions;
    }

    private long getTableCardinality(long transId, Table table, int[] keyColumnIds) throws MetadataException
    {
        if (table.getTableType() == BASE)
        {
            Map<String, Column> columnMap = getColumnMap(transId, (BaseTable) table);
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
                    long columnCard = getTableCardinality(transId, join.getLeftTable(),
                            leftKeyColumnIds.stream().mapToInt(Integer::intValue).toArray());
                    if (columnCard > cardinality)
                    {
                        cardinality = columnCard;
                    }
                }
                if (!rightKeyColumnIds.isEmpty())
                {
                    long columnCard = getTableCardinality(transId, join.getRightTable(),
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

    private double getTableInputSize(long transId, Table table) throws MetadataException
    {
        if (table.getTableType() == BASE)
        {
            Map<String, Column> columnMap = getColumnMap(transId, (BaseTable) table);
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
        return getTableInputSize(transId, leftTable) + getTableInputSize(transId, rightTable);
    }

    protected long getTableRowCount(long transId, Table table) throws MetadataException
    {
        if (table.getTableType() == BASE)
        {
            SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
            io.pixelsdb.pixels.common.metadata.domain.Table metadataTable =
                    MetadataCache.Instance().getTable(transId, schemaTableName);
            if (metadataTable == null)
            {
                metadataTable = metadataService.getTable(table.getSchemaName(), table.getTableName());
                // Issue #485: metadata cache is refreshed when the table is firstly accessed during query parsing.
            }
            return metadataTable.getRowCount();
        }
        checkArgument(table.getTableType() == JOINED, "the table is not a base or joined table");
        JoinedTable joinedTable = (JoinedTable) table;
        Table leftTable = joinedTable.getJoin().getLeftTable();
        Table rightTable = joinedTable.getJoin().getRightTable();
        // We estimate the number of rows in the joined table by the max of the two tables' row count.
        return Math.max(getTableRowCount(transId, leftTable), getTableRowCount(transId, rightTable));
    }

    public double getTableSelectivity(long transId, Table table) throws MetadataException, InvalidProtocolBufferException
    {
        if (!this.selectivityEnabled)
        {
            return 1.0;
        }

        if (table.getTableType() == BASE)
        {
            Map<String, Column> columnMap = getColumnMap(transId, (BaseTable) table);
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
                        logger.debug(String.format("column %s min: %d, max: %d, selectivity: %f, columnFilter: %s",
                                column.getName(), dateStatistic.getMinimum(), dateStatistic.getMaximum(),
                                s, JSON.toJSONString(columnFilter)));
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
        return Math.min(getTableSelectivity(transId, leftTable), getTableSelectivity(transId, rightTable));
    }

    private Map<String, Column> getColumnMap(long transId, BaseTable table) throws MetadataException
    {
        SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
        Map<String, Column> columnMap;
        // Issue #485: for consistency issue, we should not cache the column map outside the metadata cache.
        List<Column> columns = MetadataCache.Instance().getTableColumns(transId, schemaTableName);
        if (columns == null)
        {
            columns = metadataService.getColumns(table.getSchemaName(), table.getTableName(), true);
            // Issue #485: metadata cache is refreshed when the table is firstly accessed during query parsing.
        }
        columnMap = new HashMap<>(columns.size());
        for (Column column : columns)
        {
            columnMap.put(column.getName(), column);
        }
        return columnMap;
    }
}
