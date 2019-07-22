package io.pixelsdb.pixels.presto.split;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.split.index
 * @ClassName: IndexEntry
 * @Description:
 * @author: tao
 * @date: Create in 2018-06-19 14:46
 **/
public class IndexEntry
{
    private String schemaName;
    private String tableName;

    public IndexEntry(String schemaName, String tableName)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexEntry that = (IndexEntry) o;

        if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) return false;
        return tableName != null ? tableName.equals(that.tableName) : that.tableName == null;
    }

    @Override
    public int hashCode()
    {
        int result = schemaName != null ? schemaName.hashCode() : 0;
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        return result;
    }
}
