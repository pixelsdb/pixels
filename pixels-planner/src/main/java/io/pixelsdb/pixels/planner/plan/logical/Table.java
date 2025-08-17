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
package io.pixelsdb.pixels.planner.plan.logical;

/**
 * The interface of tables in pixels-turbo. A table is a (might be materialized) view of the result of a query operator.
 * @author hank
 * @create 2022-05-26
 */
public interface Table
{
    enum TableType
    {
        BASE, JOINED, AGGREGATED
    }

    public TableType getTableType();

    public String getSchemaName();

    public String getTableName();

    public String getTableAlias();

    /**
     * @return the names of the columns that are read from the table.
     */
    public String[] getColumnNames();
}
