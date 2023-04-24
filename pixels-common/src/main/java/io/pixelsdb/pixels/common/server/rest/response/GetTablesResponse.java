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
package io.pixelsdb.pixels.common.server.rest.response;

import io.pixelsdb.pixels.common.metadata.domain.Table;

import java.util.List;

/**
 * @author hank
 * @create 2023-04-21
 */
public class GetTablesResponse
{
    private String schemaName;
    private List<Table> tables;

    /**
     * Default constructor for Jackson.
     */
    public GetTablesResponse() { }

    public GetTablesResponse(String schemaName, List<Table> tables)
    {
        this.schemaName = schemaName;
        this.tables = tables;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public List<Table> getTables()
    {
        return tables;
    }

    public void setTables(List<Table> tables)
    {
        this.tables = tables;
    }
}
