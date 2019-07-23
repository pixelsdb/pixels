package io.pixelsdb.pixels.common.metadata.domain;

import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.HashSet;
import java.util.Set;

public class Schema extends Base
{
    private String name;
    private String desc;
    private Set<Long> tableIds = new HashSet<>();

    public Schema()
    {
    }

    public Schema(MetadataProto.Schema schema)
    {
        this.name = schema.getName();
        this.desc = schema.getDesc();
        this.tableIds.addAll(schema.getTableIdsList());
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getDesc()
    {
        return desc;
    }

    public void setDesc(String desc)
    {
        this.desc = desc;
    }

    public Set<Long> getTableIds()
    {
        return tableIds;
    }

    public void setTables(Set<Long> tableIds)
    {
        this.tableIds = tableIds;
    }

    public void addTableId(long tableId)
    {
        this.tableIds.add(tableId);
    }

    @Override
    public String toString()
    {
        return "Schema{" +
                "name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                '}';
    }
}
