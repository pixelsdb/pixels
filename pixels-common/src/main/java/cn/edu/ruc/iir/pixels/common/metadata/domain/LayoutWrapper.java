package cn.edu.ruc.iir.pixels.common.metadata.domain;

import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
import com.alibaba.fastjson.JSON;

public class LayoutWrapper
{
    private MetadataProto.Layout layout;

    public LayoutWrapper (MetadataProto.Layout layout)
    {
        this.layout = layout;
    }

    public int getVersion()
    {
        return this.layout.getVersion();
    }

    public long getCreateAt()
    {
        return this.layout.getCreateAt();
    }

    public boolean isWritable()
    {
        return this.layout.getPermission() == MetadataProto.Layout.PERMISSION.READWRITE;
    }

    public boolean isReadable()
    {
        return this.layout.getPermission() == MetadataProto.Layout.PERMISSION.READWRITE ||
                this.layout.getPermission() == MetadataProto.Layout.PERMISSION.READONLY;
    }

    public String getOrder()
    {
        return this.layout.getOrder();
    }

    public Order getOrderObject()
    {
        return JSON.parseObject(this.layout.getOrder(), Order.class);
    }

    public String getOrderPath()
    {
        return this.layout.getOrderPath();
    }

    public String getCompact()
    {
        return this.layout.getCompact();
    }

    public Compact getCompactObject()
    {
        return JSON.parseObject(this.layout.getCompact(), Compact.class);
    }

    public String getCompactPath()
    {
        return this.layout.getCompactPath();
    }

    public String getSplits()
    {
        return this.layout.getSplits();
    }

    public Splits getSplitsObject()
    {
        return JSON.parseObject(this.layout.getSplits(), Splits.class);
    }

    public long getTableId()
    {
        return this.layout.getTableId();
    }
}
