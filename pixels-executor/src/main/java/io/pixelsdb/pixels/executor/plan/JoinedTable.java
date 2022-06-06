package io.pixelsdb.pixels.executor.plan;

import com.google.common.base.Objects;

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
    /**
     * Whether the {@link #columnNames} includes the key columns from each joined table.
     */
    private final boolean includeKeyColumns;
    private final Join join;

    public JoinedTable(String schemaName, String tableName, String tableAlias,
                       String[] columnNames, boolean includeKeyColumns, Join join)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.columnNames = columnNames;
        this.includeKeyColumns = includeKeyColumns;
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

    public boolean isIncludeKeyColumns()
    {
        return includeKeyColumns;
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
