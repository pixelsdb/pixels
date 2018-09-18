package cn.edu.ruc.iir.pixels.common.metadata.domain;

import java.util.HashSet;
import java.util.Set;

public class Schema extends Base
{
    private static final long serialVersionUID = -9007336459109419883L;
    private String name;
    private String desc;
    private Set<Table> tables = new HashSet<>();

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

}
