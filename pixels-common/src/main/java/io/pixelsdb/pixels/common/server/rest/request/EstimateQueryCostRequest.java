/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.common.server.rest.request;

import io.pixelsdb.pixels.common.server.ExecutionHint;

/**
 * @author hank
 * @create 2023-05-24
 */
public class EstimateQueryCostRequest
{
    private String query;
    private ExecutionHint executionHint;

    /**
     * Default constructor for Jackson.
     */
    public EstimateQueryCostRequest() { }

    public EstimateQueryCostRequest(String query, ExecutionHint executionHint)
    {
        this.query = query;
        this.executionHint = executionHint;
    }

    public String getQuery()
    {
        return query;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public ExecutionHint getExecutionHint()
    {
        return executionHint;
    }

    public void setExecutionHint(ExecutionHint executionHint)
    {
        this.executionHint = executionHint;
    }
}
