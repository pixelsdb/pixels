package io.pixelsdb.pixels.optimizer.plan;

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

    /**
     * The {@link JoinedTable#columnNames} of this class is constructed by the colum alias
     * of the left and right table. The column alias from the smaller table always come first.
     *
     * @param schemaName the schema name
     * @param tableName the table name
     * @param tableAlias the table alias
     * @param join the join between the left and right table
     */
    public JoinedTable(String schemaName, String tableName, String tableAlias, Join join)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
        {
            this.columnNames = ObjectArrays.concat(
                    join.getLeftColumnAlias(), join.getRightColumnAlias(), String.class);
        }
        else
        {
            this.columnNames = ObjectArrays.concat(
                    join.getRightColumnAlias(), join.getLeftColumnAlias(), String.class);
        }
        this.join = join;
    }

    @Override
    public TableType getTableType()
    {
        return TableType.JOINED;
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
