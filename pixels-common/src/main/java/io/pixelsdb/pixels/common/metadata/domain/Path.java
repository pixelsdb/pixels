/*
 * Copyright 2023 PixelsDB.
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

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The path where data objects/files of a table been store.
 * @author hank
 * @create 2023-06-09
 */
public class Path extends Base
{
    private String uri;
    private boolean isCompact;
    private long layoutId;
    private long rangeId = 0; // 0 is an invalid id in SQL

    public Path() { }

    public Path(MetadataProto.Path path)
    {
        this.setId(path.getId());
        this.uri = path.getUri();
        this.isCompact = path.getIsCompact();
        this.layoutId = path.getLayoutId();
        if (path.hasRangeId())
        {
            this.rangeId = path.getRangeId();
        }
        else
        {
            this.rangeId = 0;
        }
    }

    public static List<Path> convertPaths(List<MetadataProto.Path> protoPaths)
    {
        requireNonNull(protoPaths, "protoPaths is null");
        ImmutableList.Builder<Path> pathsBuilder =
                ImmutableList.builderWithExpectedSize(protoPaths.size());
        for (MetadataProto.Path protoPath : protoPaths)
        {
            pathsBuilder.add(new Path(protoPath));
        }
        return pathsBuilder.build();
    }

    public static List<MetadataProto.Path> revertPaths(List<Path> paths)
    {
        requireNonNull(paths, "paths is null");
        ImmutableList.Builder<MetadataProto.Path> pathsBuilder =
                ImmutableList.builderWithExpectedSize(paths.size());
        for (Path path : paths)
        {
            pathsBuilder.add(path.toProto());
        }
        return pathsBuilder.build();
    }

    public String getUri()
    {
        return uri;
    }

    public void setUri(String uri)
    {
        this.uri = uri;
    }

    public boolean isCompact()
    {
        return isCompact;
    }

    public void setCompact(boolean compact)
    {
        isCompact = compact;
    }

    public long getLayoutId()
    {
        return layoutId;
    }

    public void setLayoutId(long layoutId)
    {
        this.layoutId = layoutId;
    }

    public long getRangeId()
    {
        return rangeId;
    }

    public void setRangeId(long rangeId)
    {
        this.rangeId = rangeId;
    }

    @Override
    public String toString()
    {
        return "Path{" +
                ", pathId='" + this.getId() + '\'' +
                ", uri='" + uri + '\'' +
                ", isCompact='" + isCompact + '\'' +
                ", layoutId='" + layoutId + '\'' +
                ", rangeId='" + rangeId + '\'' + '}';
    }

    @Override
    public MetadataProto.Path toProto()
    {
        MetadataProto.Path.Builder builder = MetadataProto.Path.newBuilder()
                .setId(this.getId()).setUri(this.uri).setIsCompact(this.isCompact).setLayoutId(this.layoutId);
        if (this.rangeId > 0)
        {
            builder.setRangeId(this.rangeId);
        }
        return builder.build();
    }
}