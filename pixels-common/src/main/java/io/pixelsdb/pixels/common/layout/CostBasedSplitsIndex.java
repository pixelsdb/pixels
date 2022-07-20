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
package io.pixelsdb.pixels.common.layout;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The splits index that calculates the split size using the statistics.
 * @author hank
 * @date 13/07/2022
 */
public class CostBasedSplitsIndex implements SplitsIndex
{
    private static final long SPLIT_SIZE_BYTES;
    private static final long SPLIT_SIZE_ROWS;

    static
    {
        String splitSizeMB = ConfigFactory.Instance().getProperty("split.size.mb");
        SPLIT_SIZE_BYTES = Long.parseLong(splitSizeMB) * 1024L * 1024L;
        String splitSizeRows = ConfigFactory.Instance().getProperty("split.size.rows");
        SPLIT_SIZE_ROWS = Long.parseLong(splitSizeRows);
    }

    private int version;
    private final int defaultSplitSize;
    private final int maxSplitSize;
    private final MetadataService metadataService;
    private final Map<String, Column> columnMap;

    public CostBasedSplitsIndex(MetadataService metadataService, SchemaTableName schemaTableName,
                                int defaultSplitSize, int maxSplitSize)
            throws MetadataException
    {
        checkArgument(defaultSplitSize > 0, "defaultSplitSize must be positive");
        this.defaultSplitSize = defaultSplitSize;
        checkArgument(maxSplitSize >= defaultSplitSize,
                "maxSplitSize must be greater or equal to defaultSplitSize");
        this.metadataService = requireNonNull(metadataService, "metadataService is null");
        requireNonNull(schemaTableName, "schemaTableName is null");

        List<Column> columns = MetadataCache.Instance().getTableColumns(schemaTableName);
        if (columns == null)
        {
            columns = this.metadataService.getColumns(
                    schemaTableName.getSchemaName(), schemaTableName.getTableName(), true);
            MetadataCache.Instance().cacheTableColumns(schemaTableName, columns);
        }
        this.columnMap = new HashMap<>(columns.size());
        double rowGroupSize = 0, tableSize = 0;
        for (Column column : columns)
        {
            rowGroupSize += column.getChunkSize();
            tableSize += column.getSize();
            this.columnMap.put(column.getName(), column);
        }

        if (SPLIT_SIZE_ROWS > 0)
        {
            Table table = MetadataCache.Instance().getTable(schemaTableName);
            if (table == null)
            {
                table = this.metadataService.getTable(
                        schemaTableName.getSchemaName(), schemaTableName.getTableName());
                MetadataCache.Instance().cacheTable(schemaTableName, table);
            }
            double numRowGroups = Math.ceil(tableSize / rowGroupSize);
            double rowsPerRowGroup = table.getRowCount() / numRowGroups;
            checkArgument(rowsPerRowGroup > 0,
                    "Number of rows per row-group must > 0.");
            // Round the split size cap to the nearest power of 2.
            int splitSizeCap = round(SPLIT_SIZE_ROWS / rowsPerRowGroup);
            this.maxSplitSize = Math.min(maxSplitSize, splitSizeCap);
        }
        else
        {
            this.maxSplitSize = maxSplitSize;
        }
    }

    @Override
    public SplitPattern search(ColumnSet columnSet)
    {
        SplitPattern bestPattern = new SplitPattern();

        if (columnSet.isEmpty())
        {
            bestPattern.setSplitSize(this.defaultSplitSize);
            return bestPattern;
        }

        double sumChunkSize = 0;
        for (String column : columnSet.getColumns())
        {
            if (this.columnMap.containsKey(column))
            {
                sumChunkSize += columnMap.get(column).getChunkSize();
            }
        }

        // Round the split size to the nearest power of 2.
        int splitSize = round(SPLIT_SIZE_BYTES / sumChunkSize);
        if (splitSize <= maxSplitSize)
        {
            bestPattern.setSplitSize(splitSize);
        }
        else
        {
            bestPattern.setSplitSize(maxSplitSize);
        }

        return bestPattern;
    }

    /**
     * Round the origin number to the nearest power of two.
     * @param origin the origin number
     * @return the rounded integer
     */
    private static int round(double origin)
    {
        int rounded1 = (int) Math.ceil(origin);
        if (rounded1 == 1)
        {
            return rounded1;
        }
        int rounded2 = Integer.highestOneBit(rounded1 - 1) << 1;
        if (origin /rounded2 < 0.75)
        {
            return rounded2 >> 1;
        }
        return rounded2;
    }

    @Override
    public int getVersion()
    {
        return version;
    }

    @Override
    public int getMaxSplitSize()
    {
        return maxSplitSize;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }
}
