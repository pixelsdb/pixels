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

import java.util.*;

/**
 * Created at: 20/10/2021
 * Author: hank
 */
public class InvertedProjectionsIndex implements ProjectionsIndex
{
    /**
     * key: column name;
     * value: bit map
     */
    private int version;

    private Map<String, BitSet> bitMapIndex;

    private List<ProjectionPattern> projectionPatterns;

    private List<String> columnOrder;

    public InvertedProjectionsIndex(List<String> columnOrder, List<ProjectionPattern> patterns)
    {
        this.columnOrder = new ArrayList<>(columnOrder);
        this.projectionPatterns = new ArrayList<>(patterns);
        this.bitMapIndex = new HashMap<>(this.columnOrder.size());

        for (String column : this.columnOrder)
        {
            BitSet bitMap = new BitSet(this.projectionPatterns.size());
            for (int i = 0; i < this.projectionPatterns.size(); ++i)
            {
                if (this.projectionPatterns.get(i).containsColumn(column))
                {
                    bitMap.set(i, true);
                }
            }
            this.bitMapIndex.put(column, bitMap);
        }
    }

    @Override
    public ProjectionPattern search(ColumnSet columnSet)
    {
        ProjectionPattern bestPattern = null;

        if (columnSet.isEmpty() || this.projectionPatterns.isEmpty())
        {
            return null;
        }

        BitSet and = new BitSet(this.projectionPatterns.size());
        and.set(0, this.projectionPatterns.size(), true);
        for (String column : columnSet.getColumns())
        {
            BitSet bitMap = this.bitMapIndex.get(column);
            and.and(bitMap);
        }

        if (and.nextSetBit(0) < 0)
        {
            // no exact access pattern found.
            return null;
        } else
        {
            int minPatternSize = Integer.MAX_VALUE;
            int i = 0;
            while ((i = and.nextSetBit(i)) >= 0)
            {
                if (this.projectionPatterns.get(i).size() < minPatternSize)
                {
                    bestPattern = this.projectionPatterns.get(i);
                    minPatternSize = bestPattern.size();
                }
                i++;
            }
        }

        return bestPattern;
    }

    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }
}
