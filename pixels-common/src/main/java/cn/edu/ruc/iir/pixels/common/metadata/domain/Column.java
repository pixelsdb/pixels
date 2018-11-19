package cn.edu.ruc.iir.pixels.common.metadata.domain;

public class Column extends Base
{
    private static final long serialVersionUID = -7648468928635345167L;
    private String name;
    private String type;
    private double size;
    private Table table;

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
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", size=" + size +
                ", table=" + table +
                '}';
    }
}
