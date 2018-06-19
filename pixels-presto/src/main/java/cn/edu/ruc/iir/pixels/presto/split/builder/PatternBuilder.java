package cn.edu.ruc.iir.pixels.presto.split.builder;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.split.Split;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.split.SplitPattern;
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

    public static List<AccessPattern> build(Split splitInfo)
            throws IOException {
        List<AccessPattern> patterns = new ArrayList<>();
        List<SplitPattern> splitPatterns = splitInfo.getSplitPatterns();

        Set<ColumnSet> existingColumnSets = new HashSet<>();
        List<String> accessedColumns;
        for (SplitPattern splitPattern : splitPatterns) {
            accessedColumns = splitPattern.getAccessedColumns();

            AccessPattern pattern = new AccessPattern();
            for (String column : accessedColumns) {
                pattern.addColumn(column);
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
