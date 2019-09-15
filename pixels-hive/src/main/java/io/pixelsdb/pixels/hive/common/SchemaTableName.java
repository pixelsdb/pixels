/*
 * Copyright 2019 PixelsDB.
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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.hive.common;

/**
 * Created at: 19-6-30
 * Author: hank
 */
public class SchemaTableName
{
    private String schemaName;
    private String tableName;

    public SchemaTableName(String schemaName, String tableName)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    @Override
    public String toString()
    {
        return "{schema:" + schemaName + ", table:" + tableName + "}";
    }
}
