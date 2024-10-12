/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.common.metadata;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author tao
 * @author hank
 * @create 2018-06-19 14:46
 **/
public class SchemaTableName
{
    private final String schemaName;
    private final String tableName;

    public SchemaTableName(String schemaName, String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public SchemaTableName(String schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        int dot = schemaTableName.indexOf(".");
        checkArgument(dot > 0 && dot < schemaTableName.length() - 1,
                "schemaTableName " + schemaTableName + " is invalid");
        this.schemaName = schemaTableName.substring(0, dot);
        this.tableName = schemaTableName.substring(dot + 1);
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
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaTableName that = (SchemaTableName) o;
        return Objects.equal(schemaName, that.schemaName) &&
                Objects.equal(tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return "SchemaTableName{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}