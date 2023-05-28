package io.pixelsdb.pixels.parser;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;

import io.pixelsdb.pixels.core.TypeDescription;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;

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

    @Override
    public String toString()
    {
        return "PixelsTable {" + tableName + "}";
    }
}
