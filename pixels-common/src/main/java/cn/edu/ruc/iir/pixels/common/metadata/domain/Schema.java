package cn.edu.ruc.iir.pixels.common.metadata.domain;

import java.util.HashSet;
import java.util.Set;

public class Schema
{
    private int id;
    private String name;
    private String desc;
    private Set<Table> tables = new HashSet<>();

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
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

    public Set<Table> getTables()
    {
        return tables;
    }

    public void setTables(Set<Table> tables)
    {
        this.tables = tables;
    }

    public void addTable (Table table)
    {
        this.tables.add(table);
    }

    @Override
    public int hashCode()
    {
        return this.id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof Schema)
        {
            return this.id == ((Schema) o).id;
        }
        return false;
    }
}
