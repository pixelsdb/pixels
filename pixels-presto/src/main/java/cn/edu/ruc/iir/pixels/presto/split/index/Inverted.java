package cn.edu.ruc.iir.pixels.presto.split.index;


import cn.edu.ruc.iir.pixels.presto.split.domain.AccessPattern;
import cn.edu.ruc.iir.pixels.presto.split.domain.ColumnSet;

import java.util.*;

public class Inverted implements Index {
    /**
     * key: column name;
     * value: bit map
     */
    private int version;

    private Map<String, BitSet> bitMapIndex = null;

    private List<AccessPattern> queryAccessPatterns = null;

    private List<String> columnOrder = null;

    public Inverted(List<String> columnOrder, List<AccessPattern> patterns) {
        this.columnOrder = new ArrayList<>(columnOrder);
        this.queryAccessPatterns = new ArrayList<>(patterns);
        ColumnSet fullColumnSet = ColumnSet.toColumnSet(this.columnOrder);
        this.bitMapIndex = new HashMap<>(fullColumnSet.size());

        for (String column : fullColumnSet.toArrayList()) {
            BitSet bitMap = new BitSet(this.queryAccessPatterns.size());
            for (int i = 0; i < this.queryAccessPatterns.size(); ++i) {
                if (this.queryAccessPatterns.get(i).contaiansColumn(column)) {
                    bitMap.set(i, true);
                }
            }
            this.bitMapIndex.put(column, bitMap);
        }
    }

    @Override
    public AccessPattern search(ColumnSet columnSet) {
        List<String> columns = columnSet.toArrayList();
        List<BitSet> bitMaps = new ArrayList<>();
        BitSet and = new BitSet(this.queryAccessPatterns.size());
        and.set(0, this.queryAccessPatterns.size(), true);
        for (String column : columns) {
            BitSet bitMap = this.bitMapIndex.get(column);
            bitMaps.add(bitMap);
            and.and(bitMap);
        }

        AccessPattern bestPattern = null;
        if (and.nextSetBit(0) < 0) {
            // no exact access pattern found.
            // look for the minimum difference in size
            int columnSize = columnSet.size();
            int minPatternSize = Integer.MAX_VALUE;
            int temp = 0;

            for (int i = 0; i < this.queryAccessPatterns.size(); ++i) {
                temp = Math.abs(this.queryAccessPatterns.get(i).size() - columnSize);
                if (temp < minPatternSize) {
                    bestPattern = this.queryAccessPatterns.get(i);
                    minPatternSize = temp;
                }
            }
        } else {
            int minPatternSize = Integer.MAX_VALUE;
            int i = 0;
            while ((i = and.nextSetBit(i)) >= 0) {
                if (this.queryAccessPatterns.get(i).size() < minPatternSize) {
                    bestPattern = this.queryAccessPatterns.get(i);
                    minPatternSize = bestPattern.size();
                }
                i++;
            }
        }

        return bestPattern;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
