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
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregatedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;

/**
 * The input for the final aggregation.
 *
 * @author hank
 * @create 05/07/2022
 * @update 2023-04-29 move some fields to {@link AggregatedTableInfo} and {@link AggregationInfo}
 */
public class AggregationInput extends Input
{
    /**
     * The information of the aggregation input.
     */
    private AggregatedTableInfo aggregatedTableInfo;
    /**
     * The information of the aggregate operation.
     */
    private AggregationInfo aggregationInfo;
    /**
     * The output of the aggregation.
     */
    private OutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public AggregationInput()
    {
        super(-1);
    }

    public AggregationInput(long transId, AggregatedTableInfo aggregatedTableInfo,
                            AggregationInfo aggregationInfo, OutputInfo output)
    {
        super(transId);
        this.aggregatedTableInfo = aggregatedTableInfo;
        this.aggregationInfo = aggregationInfo;
        this.output = output;
    }

    public AggregatedTableInfo getAggregatedTableInfo()
    {
        return aggregatedTableInfo;
    }

    public void setAggregatedTableInfo(AggregatedTableInfo aggregatedTableInfo)
    {
        this.aggregatedTableInfo = aggregatedTableInfo;
    }

    public AggregationInfo getAggregationInfo()
    {
        return aggregationInfo;
    }

    public void setAggregationInfo(AggregationInfo aggregationInfo)
    {
        this.aggregationInfo = aggregationInfo;
    }

    public OutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(OutputInfo output)
    {
        this.output = output;
    }
}
