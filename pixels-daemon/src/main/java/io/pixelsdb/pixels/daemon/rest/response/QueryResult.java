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
package io.pixelsdb.pixels.daemon.rest.response;

import java.util.List;

/**
 * Created at: 3/17/23
 * Author: hank
 */
public class QueryResult
{
    private List<String> schema;
    private List<String> previewRows;
    private boolean hasMore;
    private double executeTimeMs;
    private double priceCents;

    public QueryResult(List<String> schema, List<String> previewRows,
                       boolean hasMore, double executeTimeMs, double priceCents)
    {
        this.schema = schema;
        this.previewRows = previewRows;
        this.hasMore = hasMore;
        this.executeTimeMs = executeTimeMs;
        this.priceCents = priceCents;
    }

    public List<String> getSchema()
    {
        return schema;
    }

    public void setSchema(List<String> schema)
    {
        this.schema = schema;
    }

    public List<String> getPreviewRows()
    {
        return previewRows;
    }

    public void setPreviewRows(List<String> previewRows)
    {
        this.previewRows = previewRows;
    }

    public boolean isHasMore()
    {
        return hasMore;
    }

    public void setHasMore(boolean hasMore)
    {
        this.hasMore = hasMore;
    }

    public double getExecuteTimeMs()
    {
        return executeTimeMs;
    }

    public void setExecuteTimeMs(double executeTimeMs)
    {
        this.executeTimeMs = executeTimeMs;
    }

    public double getPriceCents()
    {
        return priceCents;
    }

    public void setPriceCents(double priceCents)
    {
        this.priceCents = priceCents;
    }
}
