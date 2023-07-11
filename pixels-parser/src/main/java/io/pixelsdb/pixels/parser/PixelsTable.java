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
package io.pixelsdb.pixels.parser;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;

import io.pixelsdb.pixels.core.TypeDescription;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collections;
import java.util.List;

/**
 * Adaptor between pixels table and calcite table.
 */
public class PixelsTable extends AbstractQueryableTable
{
    private final String schemaName;
    private final String tableName;
    private final MetadataService metadataService;

    PixelsTable(String schemaName, String tableName, MetadataService metadataService)
    {
        super(Object[].class);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.metadataService = metadataService;
    }

    /**
     * Pixels data is not enumerable and parser does not require execution, thus only return empty enumerator.
     */
    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String s)
    {
        return null;
    }

    /**
     * Use SqlTypeFactoryImpl to maintain type system consistency with Pixels TypeDescription.
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        try
        {
            List<Column> columns = metadataService.getColumns(schemaName, tableName, false);
            for (Column c : columns)
            {
                builder.add(c.getName(), pixelsType(c, typeFactory));
            }
            return builder.build();
        } catch (MetadataException | IllegalArgumentException e)
        {
            throw new RuntimeException(e);
        }
    }

    private RelDataType pixelsType(Column column, RelDataTypeFactory typeFactory) throws IllegalArgumentException
    {
        String typeStr = column.getType();
        TypeDescription typeDesc = TypeDescription.fromString(typeStr);
        TypeDescription.Category category = typeDesc.getCategory();

        SqlTypeName typeName= toSqlTypeName(category);
        RelDataType sqlType = typeName.allowsPrecScale(true, true)
                ? typeFactory.createSqlType(typeName, typeDesc.getPrecision(), typeDesc.getScale())
                : typeName.allowsPrecScale(true, false)
                ? typeFactory.createSqlType(typeName, typeDesc.getPrecision())
                : typeFactory.createSqlType(typeName);

        return sqlType;
    }

    private static SqlTypeName toSqlTypeName(TypeDescription.Category category) {
        try {
            return SqlTypeName.valueOf(category.getPrimaryName().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported or invalid type name: " + category);
        }
    }

    /**
     * Provide accurate row count from metadata, while leaving others as the default value.
     */
    @Override
    public Statistic getStatistic()
    {
        final Table table;
        try {
            table = this.metadataService.getTable(this.schemaName, this.tableName);
        } catch (MetadataException e) {
            throw new RuntimeException(e);
        }

        return new Statistic()
        {
            @Override
            public Double getRowCount()
            {
                return (double) table.getRowCount();
            }

            @Override
            public boolean isKey(ImmutableBitSet columns)
            {
                return false;
            }

            @Override
            public List<RelCollation> getCollations()
            {
                return Collections.emptyList();
            }

            @Override
            public List<RelReferentialConstraint> getReferentialConstraints()
            {
                return Collections.emptyList();
            }
        };
    }

    @Override
    public String toString()
    {
        return "PixelsTable {" + tableName + "}";
    }
}
