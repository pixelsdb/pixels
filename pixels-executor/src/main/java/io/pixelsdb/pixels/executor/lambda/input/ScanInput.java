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

import io.pixelsdb.pixels.executor.lambda.domain.OutputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.ScanTableInfo;

/**
 * The input format for table scan.
 * @author hank
 * Created at: 11/04/2022
 */
public class ScanInput implements Input
{
    /**
     * The unique id of the query.
     */
    private long queryId;
    /**
     * The information of the table to scan.
     */
    private ScanTableInfo tableInfo;
    /**
     * The output of the scan.
     */
    private OutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public ScanInput() { }

    public ScanInput(long queryId, ScanTableInfo tableInfo, OutputInfo output)
    {
        this.queryId = queryId;
        this.tableInfo = tableInfo;
        this.output = output;
    }

    public long getQueryId()
    {
        return queryId;
    }

    public void setQueryId(long queryId)
    {
        this.queryId = queryId;
    }

    public ScanTableInfo getTableInfo()
    {
        return tableInfo;
    }

    public void setTableInfo(ScanTableInfo tableInfo)
    {
        this.tableInfo = tableInfo;
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
