/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical.domain;

import java.util.List;
import java.util.Objects;

/**
 * @author hank
 * @create 2022-06-02
 */
public class ScanTableInfo extends TableInfo
{
    /**
     * The scan inputs of the table.
     */
    private List<InputSplit> inputSplits;
    /**
     * The json string of the filter (i.e., predicates) to be used in scan.
     */
    private String filter;

    /**
     * Default constructor for Jackson.
     */
    public ScanTableInfo() { }

    public ScanTableInfo(String tableName, boolean base, String[] columnsToRead,
                         StorageInfo storageInfo, List<InputSplit> inputSplits, String filter)
    {
        super(tableName, base, columnsToRead, storageInfo);
        this.inputSplits = inputSplits;
        this.filter = filter;
    }

    private ScanTableInfo(Builder builder) {
        super(builder.tableName, builder.base, builder.columnsToRead);
        this.inputSplits = builder.inputSplits;
        this.filter = builder.filter;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public List<InputSplit> getInputSplits()
    {
        return inputSplits;
    }

    public void setInputSplits(List<InputSplit> inputSplits)
    {
        this.inputSplits = inputSplits;
    }

    public String getFilter()
    {
        return filter;
    }

    public void setFilter(String filter)
    {
        this.filter = filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ScanTableInfo that = (ScanTableInfo) o;
        return inputSplits.equals(that.inputSplits) && filter.equals(that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inputSplits, filter);
    }

    public static final class Builder {
        private String tableName;
        private boolean base;
        private String[] columnsToRead;
        private List<InputSplit> inputSplits;
        private String filter;

        private Builder() {}

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setBase(boolean base) {
            this.base = base;
            return this;
        }

        public Builder setColumnsToRead(String[] columnsToRead) {
            this.columnsToRead = columnsToRead;
            return this;
        }

        public Builder setInputSplits(List<InputSplit> inputSplits) {
            this.inputSplits = inputSplits;
            return this;
        }

        public Builder setFilter(String filter) {
            this.filter = filter;
            return this;
        }

        public ScanTableInfo build() {
            return new ScanTableInfo(this);
        }
    }
}
