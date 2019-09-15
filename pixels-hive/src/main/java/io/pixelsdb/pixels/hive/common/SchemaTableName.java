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
