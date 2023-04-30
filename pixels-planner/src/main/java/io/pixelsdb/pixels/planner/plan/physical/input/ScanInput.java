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
package io.pixelsdb.pixels.planner.plan.physical.input;

import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;

import java.util.Arrays;
import java.util.Objects;

/**
 * The input format for table scan.
 * @author hank
 * @create 2022-04-11
 */
public class ScanInput extends Input
{
    /**
     * The information of the table to scan.
     */
    private ScanTableInfo tableInfo;
    /**
     * Whether the columns in tableInfo.columnsToRead should be included in the scan output.
     */
    private boolean[] scanProjection;
    /**
     * Whether the partial aggregation exists.
     */
    private boolean partialAggregationPresent = false;
    /**
     * The information of the partial aggregation.
     */
    private PartialAggregationInfo partialAggregationInfo;

    /**
     * The output of the scan.
     */
    private OutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public ScanInput()
    {
        super(-1);
    }

    public ScanInput(long transId, ScanTableInfo tableInfo, boolean[] scanProjection,
                     boolean partialAggregationPresent, PartialAggregationInfo partialAggregationInfo, OutputInfo output)
    {
        super(transId);
        this.tableInfo = tableInfo;
        this.scanProjection = scanProjection;
        this.partialAggregationPresent = partialAggregationPresent;
        this.partialAggregationInfo = partialAggregationInfo;
        this.output = output;
    }

    public ScanInput(Builder builder) {
        super(builder.queryId);
        this.tableInfo = builder.tableInfo;
        this.scanProjection = builder.scanProjection;
        this.partialAggregationPresent = builder.partialAggregationPresent;
        this.partialAggregationInfo = builder.partialAggregationInfo;
        this.output = builder.output;
    }

    public ScanTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(ScanTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public boolean[] getScanProjection()
    {
        return scanProjection;
    }

    public void setScanProjection(boolean[] scanProjection)
    {
        this.scanProjection = scanProjection;
    }

    public boolean isPartialAggregationPresent()
    {
        return partialAggregationPresent;
    }

    public void setPartialAggregationPresent(boolean partialAggregationPresent)
    {
        this.partialAggregationPresent = partialAggregationPresent;
    }

    public PartialAggregationInfo getPartialAggregationInfo()
    {
        return partialAggregationInfo;
    }

    public void setPartialAggregationInfo(PartialAggregationInfo partialAggregationInfo)
    {
        this.partialAggregationInfo = partialAggregationInfo;
    }

    public OutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(OutputInfo output)
    {
        this.output = output;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScanInput scanInput = (ScanInput) o;
        return queryId == scanInput.queryId && partialAggregationPresent == scanInput.partialAggregationPresent && tableInfo.equals(scanInput.tableInfo) && Arrays.equals(scanProjection, scanInput.scanProjection) && partialAggregationInfo.equals(scanInput.partialAggregationInfo) && output.equals(scanInput.output);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(queryId, tableInfo, partialAggregationPresent, partialAggregationInfo, output);
        result = 31 * result + Arrays.hashCode(scanProjection);
        return result;
    }

    public static final class Builder {
        private long queryId;
        private ScanTableInfo tableInfo;
        private boolean[] scanProjection;
        private boolean partialAggregationPresent = false;
        private PartialAggregationInfo partialAggregationInfo;
        private OutputInfo output;

        private Builder() {}

        public Builder setQueryId(long queryId) {
            this.queryId = queryId;
            return this;
        }

        public Builder setScanTableInfo(ScanTableInfo tableInfo) {
            this.tableInfo = tableInfo;
            return this;
        }

        public Builder setScanProjection(boolean[] scanProjection) {
            this.scanProjection = scanProjection;
            return this;
        }

        public Builder setPartialAggregationPresent(boolean partialAggregationPresent) {
            this.partialAggregationPresent = partialAggregationPresent;
            return this;
        }

        public Builder setPartialAggregationInfo(PartialAggregationInfo partialAggregationInfo) {
            this.partialAggregationInfo = partialAggregationInfo;
            return this;
        }

        public Builder setOutput(OutputInfo output) {
            this.output = output;
            return this;
        }

        public ScanInput build() {
            return new ScanInput(this);
        }
    }
}
