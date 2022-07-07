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
package io.pixelsdb.pixels.executor.lambda.input;

import io.pixelsdb.pixels.executor.lambda.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.executor.lambda.domain.MultiOutputInfo;

/**
 * @author hank
 * @date 22/05/2022
 */
public class JoinInput extends Input
{
    /**
     * Whether the partial aggregation exists.
     */
    private boolean partialAggregationPresent = false;
    /**
     * The information of the partial aggregation.
     */
    private PartialAggregationInfo partialAggregationInfo;

    /**
     * The information of the join output files.<br/>
     * <b>Note: </b>for inner, right-outer, and natural joins, the number of output files
     * should be consistent with the number of input splits in right table. For left-outer
     * and full-outer joins, there is an additional output file for the left-outer records.
     */
    private MultiOutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public JoinInput() {}

    public JoinInput(boolean partialAggregationPresent, PartialAggregationInfo partialAggregationInfo, MultiOutputInfo output)
    {
        this.partialAggregationPresent = partialAggregationPresent;
        this.partialAggregationInfo = partialAggregationInfo;
        this.output = output;
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

    /**
     * Get the information about the join output.
     * @return the join output information
     */
    public MultiOutputInfo getOutput()
    {
        return output;
    }

    /**
     * Set the information about the join output.
     * @param output the join output
     */
    public void setOutput(MultiOutputInfo output)
    {
        this.output = output;
    }
}
