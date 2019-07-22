package io.pixelsdb.pixels.presto.split;


import java.util.*;

public class Inverted implements Index
{
    /**
     * key: column name;
     * value: bit map
     */
    private int version;

    private final int defaultSplitSize;

    private Map<String, BitSet> bitMapIndex = null;

    private List<AccessPattern> queryAccessPatterns = null;

    private List<String> columnOrder = null;

    public Inverted(List<String> columnOrder, List<AccessPattern> patterns, int defaultSplitSize)
    {
        this.defaultSplitSize = defaultSplitSize;
        this.columnOrder = new ArrayList<>(columnOrder);
        this.queryAccessPatterns = new ArrayList<>(patterns);
        this.bitMapIndex = new HashMap<>(this.columnOrder.size());

        for (String column : this.columnOrder)
        {
            BitSet bitMap = new BitSet(this.queryAccessPatterns.size());
            for (int i = 0; i < this.queryAccessPatterns.size(); ++i)
            {
                if (this.queryAccessPatterns.get(i).contaiansColumn(column))
                {
                    bitMap.set(i, true);
                }
            }
            this.bitMapIndex.put(column, bitMap);
        }
    }

    @Override
    public AccessPattern search(ColumnSet columnSet)
    {
        AccessPattern bestPattern = null;

        if (columnSet.isEmpty())
        {
            bestPattern = new AccessPattern();
            bestPattern.setSplitSize(this.defaultSplitSize);
            return bestPattern;
        }

        List<BitSet> bitMaps = new ArrayList<>();
        BitSet and = new BitSet(this.queryAccessPatterns.size());
        and.set(0, this.queryAccessPatterns.size(), true);
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

            for (int i = 0; i < this.queryAccessPatterns.size(); ++i)
            {
                temp = Math.abs(this.queryAccessPatterns.get(i).size() - numColumns);
                if (temp < minPatternSize)
                {
                    bestPattern = this.queryAccessPatterns.get(i);
                    minPatternSize = temp;
                }
            }
        } else
        {
            int minPatternSize = Integer.MAX_VALUE;
            int i = 0;
            while ((i = and.nextSetBit(i)) >= 0)
            {
                if (this.queryAccessPatterns.get(i).size() < minPatternSize)
                {
                    bestPattern = this.queryAccessPatterns.get(i);
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
