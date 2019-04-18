package cn.edu.ruc.iir.pixels.common.metadata.domain;

import cn.edu.ruc.iir.pixels.daemon.MetadataProto;

public class Column extends Base
{
    private String name;
    private String type;
    private double size;
    private long tableId;

    public Column () { }

    public Column (MetadataProto.Column column)
    {
        this.setId(column.getId());
        this.name = column.getName();
        this.type = column.getType();
        this.size = column.getSize();
        this.tableId = column.getTableId();
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

    public long getTableId()
    {
        return tableId;
    }

    public void setTableId(long tableId)
    {
        this.tableId = tableId;
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", size=" + size + '\'' +
                ", tableId=" + tableId +
                '}';
    }
}
