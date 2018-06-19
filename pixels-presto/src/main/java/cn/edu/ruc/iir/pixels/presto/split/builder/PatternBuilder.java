package cn.edu.ruc.iir.pixels.presto.split.builder;

import cn.edu.ruc.iir.pixels.common.metadata.domain.SplitPattern;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Splits;
import cn.edu.ruc.iir.pixels.presto.split.domain.AccessPattern;
import cn.edu.ruc.iir.pixels.presto.split.domain.ColumnSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PatternBuilder {
    private PatternBuilder() {

    }

    public static List<AccessPattern> build(List<String> columns, Splits splitInfo)
            throws IOException {
        List<AccessPattern> patterns = new ArrayList<>();
        List<SplitPattern> splitPatterns = splitInfo.getSplitPatterns();

        Set<ColumnSet> existingColumnSets = new HashSet<>();
        List<Integer> accessedColumns;
        for (SplitPattern splitPattern : splitPatterns) {
            accessedColumns = splitPattern.getAccessedColumns();

            AccessPattern pattern = new AccessPattern();
            for (int column : accessedColumns) {
                pattern.addColumn(columns.get(column));
            }
            // set split size of each pattern
            pattern.setSplitSize(splitPattern.getNumRowGroupInSplit());

            ColumnSet columnSet = pattern.getColumnSet();

            if (!existingColumnSets.contains(columnSet)) {
                patterns.add(pattern);
                existingColumnSets.add(columnSet);
            }
        }
        return patterns;

    }
}
