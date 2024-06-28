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

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.pixelsdb.pixels.common.metadata.domain.Permission.convertPermission;

public class Layout extends Base
{
    private long version;
    private long createAt;
    private Permission permission;
    private Ordered ordered;
    private String orderedJson;
    private List<Path> orderedPaths;
    private String[] orderedPathUris;
    private Compact compact;
    private String compactJson;
    private List<Path> compactPaths;
    private String[] compactPathUris;
    private Splits splits;
    private String splitsJson;
    private Projections projections;
    private String projectionsJson;
    private Map<Long, Path> projectionPaths;
    private String[] projectionPathUris;
    private long tableId;

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
        this.orderedPaths = Path.convertPaths(layout.getOrderedPathsList());
        this.orderedPathUris = new String[this.orderedPaths.size()];
        for (int i = 0; i < this.orderedPathUris.length; ++i)
        {
            this.orderedPathUris[i] = this.orderedPaths.get(i).getUri();
        }
        this.compactJson = layout.getCompact();
        this.compact = JSON.parseObject(this.compactJson, Compact.class);
        this.compactPaths = Path.convertPaths(layout.getCompactPathsList());
        this.compactPathUris = new String[this.compactPaths.size()];
        for (int i = 0; i < this.compactPathUris.length; ++i)
        {
            this.compactPathUris[i] = this.compactPaths.get(i).getUri();
        }
        this.splitsJson = layout.getSplits();
        this.splits = JSON.parseObject(this.splitsJson, Splits.class);
        this.projectionsJson = layout.getProjections();
        this.projections = JSON.parseObject(this.projectionsJson, Projections.class);
        List<Path> paths = Path.convertPaths(layout.getProjectionPathsList());
        this.projectionPaths = new HashMap<>(paths.size());
        this.projectionPathUris = new String[paths.size()];
        for (int i = 0; i < this.projectionPathUris.length; ++i)
        {
            Path path = paths.get(i);
            this.projectionPathUris[i] = path.getUri();
            this.projectionPaths.put(path.getId(), path);
        }
        this.tableId = layout.getTableId();
    }

    public long getVersion()
    {
        return version;
    }

    public void setVersion(long version)
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

    public List<Path> getOrderedPaths()
    {
        return orderedPaths;
    }

    public String[] getOrderedPathUris()
    {
        if (this.orderedPathUris == null)
        {
            this.orderedPathUris = new String[this.orderedPaths.size()];
            for (int i = 0; i < this.orderedPathUris.length; ++i)
            {
                this.orderedPathUris[i] = this.orderedPaths.get(i).getUri();
            }
        }
        return this.orderedPathUris;
    }

    public void setOrderedPaths(List<Path> orderedPaths)
    {
        this.orderedPaths = orderedPaths;
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

    public List<Path> getCompactPaths()
    {
        return compactPaths;
    }

    public String[] getCompactPathUris()
    {
        if (this.compactPathUris == null)
        {
            this.compactPathUris = new String[this.compactPaths.size()];
            for (int i = 0; i < this.compactPathUris.length; ++i)
            {
                this.compactPathUris[i] = this.compactPaths.get(i).getUri();
            }
        }
        return this.compactPathUris;
    }

    public void setCompactPaths(List<Path> compactPaths)
    {
        this.compactPaths = compactPaths;
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

    public Map<Long, Path> getProjectionPaths()
    {
        return projectionPaths;
    }

    public String[] getProjectionPathUris()
    {
        if (this.projectionPathUris == null)
        {
            this.projectionPathUris = new String[this.projectionPaths.size()];
            int i = 0;
            for (Path path : this.projectionPaths.values())
            {
                this.projectionPathUris[i++] = path.getUri();
            }
        }
        return this.projectionPathUris;
    }

    public void setProjectionPaths(Map<Long, Path> projectionPaths)
    {
        this.projectionPaths = projectionPaths;
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

    @Override
    public MetadataProto.Layout toProto()
    {
        return MetadataProto.Layout.newBuilder()
                .setId(this.getId()).setPermission(convertPermission(this.getPermission()))
                .setCreateAt(this.getCreateAt()).setSplits(this.getSplitsJson())
                .setOrdered(this.getOrderedJson()).setCompact(this.getCompactJson())
                .setVersion(this.getVersion()).setProjections(this.getProjectionsJson())
                .setTableId(this.getTableId()).build();
    }
}
