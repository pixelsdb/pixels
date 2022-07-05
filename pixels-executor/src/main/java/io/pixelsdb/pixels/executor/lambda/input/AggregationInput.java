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

import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.lambda.domain.OutputInfo;

import java.util.List;

/**
 * The input for the final aggregation.
 *
 * @author hank
 * @date 05/07/2022
 */
public class AggregationInput
{
    /**
     * The column names of the group key columns in the aggregation result.
     */
    private String[] groupColumnNames;
    /**
     * The column names of the aggregated columns in the aggregation result.
     */
    private String[] resultColumnNames;
    /**
     * The aggregation functions, in the same order of resultColumnNames.
     */
    private FunctionType[] functionTypes;
    /**
     * The paths of the partial aggregated files.
     */
    private List<String> inputFiles;

    /**
     * The output of the aggregation.
     */
    private OutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public AggregationInput()
    {
    }

    public AggregationInput(String[] groupColumnNames, String[] resultColumnNames,
                            FunctionType[] functionTypes, List<String> inputFiles,
                            OutputInfo output)
    {
        this.groupColumnNames = groupColumnNames;
        this.resultColumnNames = resultColumnNames;
        this.functionTypes = functionTypes;
        this.inputFiles = inputFiles;
        this.output = output;
    }

    public String[] getGroupColumnNames()
    {
        return groupColumnNames;
    }

    public void setGroupColumnNames(String[] groupColumnNames)
    {
        this.groupColumnNames = groupColumnNames;
    }

    public String[] getResultColumnNames()
    {
        return resultColumnNames;
    }

    public void setResultColumnNames(String[] resultColumnNames)
    {
        this.resultColumnNames = resultColumnNames;
    }

    public FunctionType[] getFunctionTypes()
    {
        return functionTypes;
    }

    public void setFunctionTypes(FunctionType[] functionTypes)
    {
        this.functionTypes = functionTypes;
    }

    public List<String> getInputFiles()
    {
        return inputFiles;
    }

    public void setInputFiles(List<String> inputFiles)
    {
        this.inputFiles = inputFiles;
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
