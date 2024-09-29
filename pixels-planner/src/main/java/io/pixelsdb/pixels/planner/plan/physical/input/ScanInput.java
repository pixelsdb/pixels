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

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

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
        super(-1, -1);
    }

    public ScanInput(long transId, long timestamp, ScanTableInfo tableInfo, boolean[] scanProjection,
                     boolean partialAggregationPresent, PartialAggregationInfo partialAggregationInfo, OutputInfo output)
    {
        super(transId, timestamp);
        this.tableInfo = tableInfo;
        this.scanProjection = scanProjection;
        this.partialAggregationPresent = partialAggregationPresent;
        this.partialAggregationInfo = partialAggregationInfo;
        this.output = output;
    }

    public ScanTableInfo getTableInfo()
    {
        return tableInfo;
    }

    public void setTableInfo(ScanTableInfo tableInfo)
    {
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

    public static List<String> generateOutputPaths(String outputFolder, int numSplits)
    {
        requireNonNull(outputFolder, "outputFolder is null");
        checkArgument(numSplits > 0, "numSplits is non-positive");
        ImmutableList.Builder<String> builder = ImmutableList.builderWithExpectedSize(numSplits);
        if (!outputFolder.endsWith("/"))
        {
            outputFolder += "/";
        }
        for (int i = 0; i < numSplits; ++i)
        {
            builder.add(outputFolder + "scan_" + i);
        }
        return builder.build();
    }

    /**
     * Generate the absolute paths of the output files for a scan input.
     * @param scanInput the scan input
     * @return the absolute output file paths
     */
    public static List<String> generateOutputPaths(ScanInput scanInput)
    {
        requireNonNull(scanInput, "scanInput is null");
        String outputFolder = scanInput.getOutput().getPath();
        int numSplits = scanInput.getTableInfo().getInputSplits().size();
        return generateOutputPaths(outputFolder, numSplits);
    }
}
