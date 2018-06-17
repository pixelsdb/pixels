package cn.edu.ruc.iir.pixels.common.metadata;

public class Layout
{
    private int id;
    private int version;
    private long createAt;
    private int permission;
    private String order;
    private String orderPath;
    private String compact;
    private String compactPath;
    private String splits;
    private Table table;

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }

    public long getCreateAt()
    {
        return createAt;
    }

    public void setCreateAt(long createAt)
    {
        this.createAt = createAt;
    }

    public boolean isWritable()
    {
        return this.permission > 0;
    }

    public boolean isReadable()
    {
        return this.permission >= 0;
    }

    public int getPermission()
    {
        return permission;
    }

    public void setPermission(int permission)
    {
        this.permission = permission;
    }

    public String getOrder()
    {
        return order;
    }

    public void setOrder(String order)
    {
        this.order = order;
    }

    public String getOrderPath()
    {
        return orderPath;
    }

    public void setOrderPath(String orderPath)
    {
        this.orderPath = orderPath;
    }

    public String getCompact()
    {
        return compact;
    }

    public void setCompact(String compact)
    {
        this.compact = compact;
    }

    public String getCompactPath()
    {
        return compactPath;
    }

    public void setCompactPath(String compactPath)
    {
        this.compactPath = compactPath;
    }

    public String getSplits()
    {
        return splits;
    }

    public void setSplits(String split)
    {
        this.splits = split;
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
        if (o instanceof Layout)
        {
            return this.id == ((Layout) o).id;
        }
        return false;
    }
}
