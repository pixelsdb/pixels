package cn.edu.ruc.iir.pixels.presto.split.domain;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ColumnSet {

    private Set<String> columns = null;

    public ColumnSet() {
        this.columns = new HashSet<>();
    }

    public ColumnSet(Set<String> columns) {
        this.columns = new HashSet<>(columns);
    }

    public void addColumn(String column) {
        this.columns.add(column);
    }

    public boolean contains(String column) {
        return this.columns.contains(column);
    }

    public int size() {
        return this.columns.size();
    }

    public List<String> toArrayList() {
        return new ArrayList<>(this.columns);
    }

    public static ColumnSet toColumnSet(List<String> columnOrder) {
        ColumnSet columnSet = new ColumnSet();
        for (String column : columnOrder) {
            columnSet.addColumn(column);
        }
        return columnSet;
    }

    @Override
    public int hashCode() {
        return this.columns.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof ColumnSet) {
            ColumnSet set = (ColumnSet) o;
            for (String column : set.columns) {
                if (!this.columns.contains(column)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new ColumnSet(this.columns);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String column : this.columns) {
            builder.append(column).append(',');
        }
        return builder.substring(0, builder.length() - 1);
    }
}
