/*
 * Copyright 2021 PixelsDB.
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

import io.pixelsdb.pixels.common.metadata.domain.OriginProjectionPattern;
import io.pixelsdb.pixels.common.metadata.domain.Projections;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created at: 20/10/2021
 * Author: hank
 */
public class ProjectionPattern
{
    // it seems that this.pattern can be a Set.
    private final ColumnSet columnSet;
    private String[] paths;

    public ProjectionPattern()
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

    public void setPaths(String... paths)
    {
        this.paths = paths;
    }

    public String[] getPaths()
    {
        return paths;
    }

    public boolean containsColumn(String column)
    {
        return this.columnSet.contains(column);
    }

    @Override
    public String toString()
    {
        if (this.columnSet.isEmpty())
        {
            return "paths: " + paths + ", pattern is empty";
        }
        StringBuilder builder = new StringBuilder();
        for (String column : this.columnSet.getColumns())
        {
            builder.append(",").append(column);
        }
        return "paths: " + paths + ", pattern: " + builder.substring(1);
    }

    public static List<ProjectionPattern> buildPatterns(List<String> columns, Projections projectionInfo)
    {
        List<ProjectionPattern> patterns = new ArrayList<>();
        List<OriginProjectionPattern> originProjPatterns =
                projectionInfo.getProjectionPatterns();

        Set<ColumnSet> existingColumnSets = new HashSet<>();
        List<Integer> accessedColumns;
        for (OriginProjectionPattern originProjPattern : originProjPatterns)
        {
            accessedColumns = originProjPattern.getAccessedColumns();

            ProjectionPattern pattern = new ProjectionPattern();
            for (int column : accessedColumns)
            {
                pattern.addColumn(columns.get(column));
            }
            // set split size of each pattern
            pattern.setPaths(originProjPattern.getPaths());

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
