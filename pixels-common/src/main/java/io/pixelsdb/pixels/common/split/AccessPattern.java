/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.common.split;

import io.pixelsdb.pixels.common.metadata.domain.SplitPattern;
import io.pixelsdb.pixels.common.metadata.domain.Splits;

import java.io.IOException;
import java.util.*;

/**
 * @author hank
 */
public class AccessPattern
{
    // it seems that this.pattern can be a Set.
    private ColumnSet columnSet = null;
    private int splitSize;

    public AccessPattern()
    {
        this.columnSet = new ColumnSet();
    }

    public void addColumn(String column)
    {
        this.columnSet.addColumn(column);
    }

    public int size()
    {
        return this.columnSet.size();
    }

    public ColumnSet getColumnSet()
    {
        return this.columnSet;
    }

    public void setSplitSize(int splitSize)
    {
        this.splitSize = splitSize;
    }

    public int getSplitSize()
    {
        return splitSize;
    }

    public boolean contaiansColumn(String column)
    {
        return this.columnSet.contains(column);
    }

    @Override
    public String toString()
    {
        if (this.columnSet.isEmpty())
        {
            return "splitSize: " + splitSize + ", pattern is empty";
        }
        StringBuilder builder = new StringBuilder();
        for (String column : this.columnSet.getColumns())
        {
            builder.append(",").append(column);
        }
        return "splitSize: " + splitSize + ", pattern: " + builder.substring(1);
    }

    public static List<AccessPattern> buildPatterns(List<String> columns, Splits splitInfo)
            throws IOException
    {
        List<AccessPattern> patterns = new ArrayList<>();
        List<SplitPattern> splitPatterns = splitInfo.getSplitPatterns();

        Set<ColumnSet> existingColumnSets = new HashSet<>();
        List<Integer> accessedColumns;
        for (SplitPattern splitPattern : splitPatterns)
        {
            accessedColumns = splitPattern.getAccessedColumns();

            AccessPattern pattern = new AccessPattern();
            for (int column : accessedColumns)
            {
                pattern.addColumn(columns.get(column));
            }
            // set split size of each pattern
            pattern.setSplitSize(splitPattern.getNumRowGroupInSplit());

            ColumnSet columnSet = pattern.getColumnSet();

            if (!existingColumnSets.contains(columnSet))
            {
                patterns.add(pattern);
                existingColumnSets.add(columnSet);
            }
        }
        return patterns;
    }
}
