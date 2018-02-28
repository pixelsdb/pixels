package cn.edu.ruc.iir.pixels.presto.split.domain;


import java.util.*;

public class AccessPattern {
    // it seems that this.pattern can be a Set.
    private List<String> pattern = null;
    private int splitSize;

    public AccessPattern() {
        this.pattern = new ArrayList<>();
    }

    public AccessPattern(List<String> pattern) {
        this();
        for (String column : this.pattern) {
            this.addColumn(column);
        }
    }

    public void addColumn(String column) {
        this.pattern.add(column);
    }

    public int size() {
        return this.pattern.size();
    }

    public ColumnSet getColumnSet() {
        return new ColumnSet(new HashSet<>(this.pattern));
    }

    public void setSplitSize(int splitSize) {
        this.splitSize = splitSize;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public boolean contaiansColumn(String column) {
        return this.pattern.contains(column);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String column : this.pattern) {
            builder.append(",").append(column);
        }
        return "splitSize: " + splitSize + "\npattern: " + builder.substring(1);
    }
}
