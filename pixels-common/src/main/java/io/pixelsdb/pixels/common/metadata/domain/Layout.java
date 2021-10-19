/*
 * Copyright 2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.metadata.domain;

import io.pixelsdb.pixels.daemon.MetadataProto;
import com.alibaba.fastjson.JSON;

public class Layout extends Base
{
    public enum Permission
    {
        DISABLED,
        READ_ONLY,
        READ_WRITE,
        UNRECOGNIZED
    }

    private int version;
    private long createAt;
    private Permission permission;
    private String order;
    private String orderPath;
    private String compact;
    private String compactPath;
    private String splits;
    private String projections;
    private long tableId;
    private Order orderObj = null;
    private Compact compactObj = null;
    private Splits splitsObj = null;
    private Projections projectionsObj = null;

    public Layout()
    {
    }

    public Layout(MetadataProto.Layout layout)
    {
        this.setId(layout.getId());
        this.version = layout.getVersion();
        this.createAt = layout.getCreateAt();
        switch (layout.getPermission())
        {
            case DISABLED:
                this.permission = Permission.DISABLED;
                break;
            case READ_ONLY:
                this.permission = Permission.READ_ONLY;
                break;
            case READ_WRITE:
                this.permission = Permission.READ_WRITE;
                break;
            case UNRECOGNIZED:
                this.permission = Permission.UNRECOGNIZED;
                break;
        }
        this.order = layout.getOrder();
        this.orderPath = layout.getOrderPath();
        this.compact = layout.getCompact();
        this.compactPath = layout.getCompactPath();
        this.splits = layout.getSplits();
        this.projections = layout.getProjections();
        this.tableId = layout.getTableId();
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
        return this.permission == Permission.READ_WRITE;
    }

    public boolean isReadable()
    {
        return this.permission == Permission.READ_ONLY ||
                this.permission == Permission.READ_WRITE;
    }

    public Permission getPermission()
    {
        return permission;
    }

    public void setPermission(Permission permission)
    {
        this.permission = permission;
    }

    public String getOrder()
    {
        return order;
    }

    public Order getOrderObject()
    {
        if (this.orderObj == null)
        {
            this.orderObj = JSON.parseObject(this.order, Order.class);
        }
        return this.orderObj;
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

    public Compact getCompactObject()
    {
        if (this.compactObj == null)
        {
            this.compactObj = JSON.parseObject(this.compact, Compact.class);
        }
        return this.compactObj;
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

    public Splits getSplitsObject()
    {
        if (this.splitsObj == null)
        {
            this.splitsObj = JSON.parseObject(this.splits, Splits.class);
        }
        return this.splitsObj;
    }

    public void setSplits(String split)
    {
        this.splits = split;
    }

    public String getProjections()
    {
        return projections;
    }

    public Projections getProjectionsObject()
    {
        if (this.projectionsObj == null)
        {
            this.projectionsObj = JSON.parseObject(this.projections, Projections.class);
        }
        return this.projectionsObj;
    }

    public void setProjections(String projections)
    {
        this.projections = projections;
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
    public String toString()
    {
        return "Layout{" +
                "version=" + version +
                ", createAt=" + createAt +
                ", permission=" + permission + '\'' +
                ", order='" + order + '\'' +
                ", orderPath='" + orderPath + '\'' +
                ", compact='" + compact + '\'' +
                ", compactPath='" + compactPath + '\'' +
                ", splits='" + splits + '\'' +
                ", projections='" + projections + '\'' +
                ", tableId=" + tableId +
                '}';
    }
}
