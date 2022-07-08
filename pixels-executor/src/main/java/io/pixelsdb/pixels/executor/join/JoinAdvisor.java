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
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.plan.BaseTable;
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
    private final double broadcastThreshold;
    private final double partitionSize;
    private final double baseTableScanUnit;
    /**
     * schemaName_tableName -> (columnName -> column)
     */
    private final Map<String, Map<String, Column>> columnStatisticCache = new HashMap<>();

    private JoinAdvisor()
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        String host = configFactory.getProperty("metadata.server.host");
        int port = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        metadataService = new MetadataService(host, port);
        String thresholdStr = configFactory.getProperty("join.broadcast.threshold");
        broadcastThreshold = parseDataSize(thresholdStr);
        String partitionSizeStr = configFactory.getProperty("join.partition.size");
        partitionSize = parseDataSize(partitionSizeStr);
        String baseTableScanUnitStr = configFactory.getProperty("join.base.table.scan.unit");
        baseTableScanUnit = parseDataSize(baseTableScanUnitStr);
    }

    public JoinAlgorithm getJoinAlgorithm(Table leftTable, Table rightTable, JoinEndian joinEndian)
            throws MetadataException
    {
        double smallTableSize;
        if (joinEndian == JoinEndian.SMALL_LEFT)
        {
            smallTableSize = getTableSize(leftTable);
        }
        else
        {
            smallTableSize = getTableSize(rightTable);
        }
        if (smallTableSize >= broadcastThreshold)
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
        double leftTableSize = getTableSize(leftTable);
        double rightTableSize = getTableSize(rightTable);
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
            largeTableSize = getTableSize(leftTable);
        }
        else
        {
            largeTableSize = getTableSize(rightTable);
        }
        return (int) Math.ceil(largeTableSize / partitionSize);
    }

    /**
     * Get the table scan input split size for the base table.
     * @param table the base table
     * @param numRowGroups the total number of row groups in the base table
     * @return the number of row groups to be scanned by each task
     */
    public int getSplitSizeCap(BaseTable table, int numRowGroups) throws MetadataException
    {
        double tableSize = getTableSize(table);
        double numSplits = tableSize / baseTableScanUnit;
        double splitSize = Math.ceil(numRowGroups / numSplits);
        return roundSplitSize(splitSize);
    }

    private double getTableSize(Table table) throws MetadataException
    {
        if (table.getTableType() == BASE)
        {
            String schemaTableName = table.getSchemaName() + "_" + table.getTableName();
            Map<String, Column> columnMap;
            if (columnStatisticCache.containsKey(schemaTableName))
            {
                columnMap = columnStatisticCache.get(schemaTableName);
            }
            else
            {
                List<Column> columns = metadataService.getColumns(
                        table.getSchemaName(), table.getTableName(), true);
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
        return getTableSize(leftTable) + getTableSize(rightTable);
    }

    private double parseDataSize(String sizeStr)
    {
        double l = Double.parseDouble(sizeStr.substring(0, sizeStr.length() - 2));
        if (sizeStr.endsWith("KB"))
        {
            return l * 1024;
        }
        else if (sizeStr.endsWith("MB"))
        {
            return l * 1024 * 1024;
        }
        else if (sizeStr.endsWith("GB"))
        {
            return l * 1024 * 1024 * 1024;
        }
        else if (sizeStr.endsWith("B"))
        {
            return Double.parseDouble(sizeStr.substring(0, sizeStr.length()-1));
        }
        throw new UnsupportedOperationException("size '" + sizeStr + "' can not be parsed");
    }

    private static final double log2 = Math.log(2);

    private int roundSplitSize(double splitSize)
    {
        checkArgument(splitSize >= 1.0, "split size must >= 1.0");
        long power2 = Math.round(Math.log(splitSize) / log2);
        checkArgument(power2 >= 0.0, "split size must be a non-negative power of 2.");
        return (int) Math.pow(2, power2);
    }
}
