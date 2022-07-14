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
    private static final int SPLIT_SIZE_BYTES;

    static
    {
        String unit = ConfigFactory.Instance().getProperty("split.size.in.bytes");
        SPLIT_SIZE_BYTES = Integer.parseInt(unit);
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
        this.maxSplitSize = maxSplitSize;
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
        for (Column column : columns)
        {
            this.columnMap.put(column.getName(), column);
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

        if (sumChunkSize >= SPLIT_SIZE_BYTES)
        {
            bestPattern.setSplitSize(1);
        }
        else
        {
            int splitSize = (int) Math.ceil(SPLIT_SIZE_BYTES / sumChunkSize);
            // round the split size to be the power of 2.
            splitSize = Integer.highestOneBit(splitSize - 1) << 1;
            if (splitSize <= maxSplitSize)
            {
                bestPattern.setSplitSize(splitSize);
            }
            else
            {
                bestPattern.setSplitSize(maxSplitSize);
            }
        }

        return bestPattern;
    }

    @Override
    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }
}
