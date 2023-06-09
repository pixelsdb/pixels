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

import java.util.List;

public class Layout extends Base
{
    private int version;
    private long createAt;
    private Permission permission;
    private Ordered ordered;
    private List<Long> orderedPathIds;
    private Compact compact;
    private List<Long> compactPathIds;
    private Splits splits;
    private Projections projections;
    private long tableId;
    private String orderedJson;
    private String compactJson;
    private String splitsJson;
    private String projectionsJson;

    public Layout() { }

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
        this.orderedJson = layout.getOrdered();
        this.ordered = JSON.parseObject(this.orderedJson, Ordered.class);
        this.orderedPathIds = layout.getOrderedPathIdsList();
        this.compactJson = layout.getCompact();
        this.compact = JSON.parseObject(this.compactJson, Compact.class);
        this.compactPathIds = layout.getCompactPathIdsList();
        this.splitsJson = layout.getSplits();
        this.splits = JSON.parseObject(this.splitsJson, Splits.class);
        this.projectionsJson = layout.getProjections();
        this.projections = JSON.parseObject(this.projectionsJson, Projections.class);
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

    public Ordered getOrdered()
    {
        if (this.ordered == null)
        {
            this.ordered = JSON.parseObject(this.orderedJson, Ordered.class);
        }
        return ordered;
    }

    public String getOrderedJson()
    {
        return orderedJson;
    }

    public void setOrderedJson(String orderedJson)
    {
        this.orderedJson = orderedJson;
    }

    public List<Long> getOrderedPathIds()
    {
        return orderedPathIds;
    }

    public void setOrderedPathIds(List<Long> orderedPathIds)
    {
        this.orderedPathIds = orderedPathIds;
    }

    public Compact getCompact()
    {
        if (this.compact == null)
        {
            JSON.parseObject(this.compactJson, Compact.class);
        }
        return compact;
    }

    public String getCompactJson()
    {
        return compactJson;
    }

    public void setCompactJson(String compactJson)
    {
        this.compactJson = compactJson;
    }

    public List<Long> getCompactPathIds()
    {
        return compactPathIds;
    }

    public void setCompactPathIds(List<Long> compactPathIds)
    {
        this.compactPathIds = compactPathIds;
    }

    public Splits getSplits()
    {
        if (this.splits == null)
        {
            JSON.parseObject(this.splitsJson, Splits.class);
        }
        return splits;
    }

    public String getSplitsJson()
    {
        return this.splitsJson;
    }

    public void setSplitsJson(String splitsJson)
    {
        this.splitsJson = splitsJson;
    }

    public Projections getProjections()
    {
        if (this.projections == null)
        {
            JSON.parseObject(this.projectionsJson, Projections.class);
        }
        return projections;
    }

    public String getProjectionsJson()
    {
        return this.projectionsJson;
    }

    public void setProjectionsJson(String projectionsJson)
    {
        this.projectionsJson = projectionsJson;
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
                ", ordered='" + orderedJson + '\'' +
                ", compact='" + compactJson + '\'' +
                ", splits='" + splitsJson + '\'' +
                ", projections='" + projectionsJson + '\'' +
                ", tableId=" + tableId + '}';
    }
}
