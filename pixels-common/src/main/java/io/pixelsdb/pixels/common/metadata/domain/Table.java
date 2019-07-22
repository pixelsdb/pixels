package io.pixelsdb.pixels.common.metadata.domain;

import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.ArrayList;
import java.util.List;

public class Table extends Base
{
    private String name;
    private String type;
    private long schemaId;
    private List<Long> columnIds = new ArrayList<>();

    public Table()
    {
    }

    public Table(MetadataProto.Table table)
    {
        this.name = table.getName();
        this.type = table.getType();
        this.schemaId = table.getSchemaId();
        this.columnIds.addAll(table.getColumnIdsList());
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public long getSchemaId()
    {
        return schemaId;
    }

    public void setSchema(long schemaId)
    {
        this.schemaId = schemaId;
    }

    public List<Long> getColumnIds()
    {
        return columnIds;
    }

    public void setColumnIds(List<Long> columnIds)
    {
        this.columnIds = columnIds;
    }

    public void addColumnId(long columnId)
    {
        this.columnIds.add(columnId);
    }

    @Override
    public String toString()
    {
        return "Table{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", schemaId=" + schemaId +
                '}';
    }
}
