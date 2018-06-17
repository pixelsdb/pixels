package cn.edu.ruc.iir.pixels.common.metadata;

public class Column
{
    private int id;
    private String name;
    private String type;
    private double size;
    private Table table;

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

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public double getSize()
    {
        return size;
    }

    public void setSize(double size)
    {
        this.size = size;
    }

    public Table getTable()
    {
        return table;
    }

    public void setTable(Table table)
    {
        this.table = table;
    }

    @Override
    public int hashCode()
    {
        return this.id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof Column)
        {
            return this.id == ((Column) o).id;
        }
        return false;
    }
}
