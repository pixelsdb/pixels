package io.pixelsdb.pixels.executor.plan;

import com.google.common.base.Objects;
import com.google.common.collect.ObjectArrays;

/**
 * The table (view) of the join result.
 * @author hank
 * @date 30/05/2022
 */
public class JoinedTable implements Table
{
    private final String schemaName;
    private final String tableName;
    private final String tableAlias;
    private final String[] columnNames;
    private final Join join;

    public JoinedTable(String schemaName, String tableName, String tableAlias, Join join)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.columnNames = ObjectArrays.concat(join.getLeftColumnAlias(), join.getRightColumnAlias(), String.class);
        this.join = join;
    }

    @Override
    public boolean isBase()
    {
        return false;
    }

    @Override
    public String getSchemaName()
    {
        return schemaName;
    }

    @Override
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public String getTableAlias()
    {
        return tableAlias;
    }

    @Override
    public String[] getColumnNames()
    {
        return columnNames;
    }

    public Join getJoin()
    {
        return join;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinedTable that = (JoinedTable) o;
        return Objects.equal(schemaName, that.schemaName) &&
                Objects.equal(tableName, that.tableName) &&
                Objects.equal(tableAlias, that.tableAlias) &&
                Objects.equal(columnNames, that.columnNames) &&
                Objects.equal(join, that.join);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, tableName, tableAlias, columnNames, join);
    }
}
