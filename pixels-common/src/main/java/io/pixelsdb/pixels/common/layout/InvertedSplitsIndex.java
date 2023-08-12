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
package io.pixelsdb.pixels.common.layout;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2018-02
 */
public class InvertedSplitsIndex implements SplitsIndex
{
    private final long version;
    private final int maxSplitSize;

    /**
     * key: column name;
     * value: bit map
     */
    private final Map<String, BitSet> bitMapIndex;

    private final List<SplitPattern> querySplitPatterns;

    private List<String> columnOrder;

    public InvertedSplitsIndex(long version, List<String> columnOrder, List<SplitPattern> patterns, int maxSplitSize)
    {
        this.version = version;
        this.maxSplitSize = maxSplitSize;
        this.columnOrder = new ArrayList<>(columnOrder);
        this.querySplitPatterns = new ArrayList<>(patterns);
        this.bitMapIndex = new HashMap<>(this.columnOrder.size());

        for (String column : this.columnOrder)
        {
            BitSet bitMap = new BitSet(this.querySplitPatterns.size());
            for (int i = 0; i < this.querySplitPatterns.size(); ++i)
            {
                if (this.querySplitPatterns.get(i).containsColumn(column))
                {
                    bitMap.set(i, true);
                }
            }
            this.bitMapIndex.put(column, bitMap);
        }
    }

    @Override
    public SplitPattern search(ColumnSet columnSet)
    {
        SplitPattern bestPattern = null;

        if (columnSet.isEmpty())
        {
            bestPattern = new SplitPattern();
            bestPattern.setSplitSize(this.maxSplitSize);
            return bestPattern;
        }

        BitSet and = new BitSet(this.querySplitPatterns.size());
        and.set(0, this.querySplitPatterns.size(), true);
        for (String column : columnSet.getColumns())
        {
            BitSet bitMap = this.bitMapIndex.get(column);
            and.and(bitMap);
        }

        if (and.nextSetBit(0) < 0)
        {
            // no exact access pattern found.
            // look for the minimum difference in size
            // TODO: this is not a good strategy.
            // Instead, the access pattern with minimum difference in actual read size
            // should be used as the best access pattern. It requires that data size of each column
            // be maintained in ColumnSet.
            int numColumns = columnSet.size();
            int minPatternSize = Integer.MAX_VALUE;
            int temp;

            for (SplitPattern querySplitPattern : this.querySplitPatterns)
            {
                temp = Math.abs(querySplitPattern.size() - numColumns);
                if (temp < minPatternSize)
                {
                    bestPattern = querySplitPattern;
                    minPatternSize = temp;
                }
            }
        } else
        {
            int minPatternSize = Integer.MAX_VALUE;
            int i = 0;
            while ((i = and.nextSetBit(i)) >= 0)
            {
                if (this.querySplitPatterns.get(i).size() < minPatternSize)
                {
                    bestPattern = this.querySplitPatterns.get(i);
                    minPatternSize = bestPattern.size();
                }
                i++;
            }
        }

        requireNonNull(bestPattern, "best pattern is not found");
        if (bestPattern.getSplitSize() > maxSplitSize)
        {
            bestPattern.setSplitSize(maxSplitSize);
        }

        return bestPattern;
    }

    @Override
    public long getVersion()
    {
        return version;
    }

    @Override
    public int getMaxSplitSize()
    {
        return maxSplitSize;
    }
}
