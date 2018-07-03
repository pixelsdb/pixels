package cn.edu.ruc.iir.pixels.presto.split;


import cn.edu.ruc.iir.pixels.common.metadata.domain.SplitPattern;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Splits;

import java.io.IOException;
import java.util.*;

public class AccessPattern
{
    // it seems that this.pattern can be a Set.
    private List<String> pattern = new ArrayList<>();
    private int splitSize;

    public void addColumn(String column)
    {
        this.pattern.add(column);
    }

    public int size()
    {
        return this.pattern.size();
    }

    public ColumnSet getColumnSet()
    {
        return new ColumnSet(new HashSet<>(this.pattern));
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
        return this.pattern.contains(column);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for (String column : this.pattern)
        {
            builder.append(",").append(column);
        }
        return "splitSize: " + splitSize + "\npattern: " + builder.substring(1);
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
