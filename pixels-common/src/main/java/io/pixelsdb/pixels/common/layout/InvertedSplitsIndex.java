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

/**
 * @author hank
 */
public class InvertedSplitsIndex implements SplitsIndex
{
    /**
     * key: column name;
     * value: bit map
     */
    private int version;

    private final int defaultSplitSize;

    private Map<String, BitSet> bitMapIndex = null;

    private List<SplitPattern> querySplitPatterns = null;

    private List<String> columnOrder = null;

    public InvertedSplitsIndex(List<String> columnOrder, List<SplitPattern> patterns, int defaultSplitSize)
    {
        this.defaultSplitSize = defaultSplitSize;
        this.columnOrder = new ArrayList<>(columnOrder);
        this.querySplitPatterns = new ArrayList<>(patterns);
        this.bitMapIndex = new HashMap<>(this.columnOrder.size());

        for (String column : this.columnOrder)
        {
            BitSet bitMap = new BitSet(this.querySplitPatterns.size());
            for (int i = 0; i < this.querySplitPatterns.size(); ++i)
            {
                if (this.querySplitPatterns.get(i).contaiansColumn(column))
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
            bestPattern.setSplitSize(this.defaultSplitSize);
            return bestPattern;
        }

        List<BitSet> bitMaps = new ArrayList<>();
        BitSet and = new BitSet(this.querySplitPatterns.size());
        and.set(0, this.querySplitPatterns.size(), true);
        for (String column : columnSet.getColumns())
        {
            BitSet bitMap = this.bitMapIndex.get(column);
            bitMaps.add(bitMap);
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

            for (int i = 0; i < this.querySplitPatterns.size(); ++i)
            {
                temp = Math.abs(this.querySplitPatterns.get(i).size() - numColumns);
                if (temp < minPatternSize)
                {
                    bestPattern = this.querySplitPatterns.get(i);
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
