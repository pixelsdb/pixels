/*
 * Copyright 2024 PixelsDB.
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
 * @author hank
 * @create 2024-06-08
 */
public class File extends Base
{
    private String name;
    private int numRowGroup;
    private long pathId;

    public File()
    {
    }

    public File(MetadataProto.File file)
    {
        this.setId(file.getId());
        this.name = file.getName();
        this.numRowGroup = file.getNumRowGroup();
        this.pathId = file.getPathId();
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public int getNumRowGroup()
    {
        return numRowGroup;
    }

    public void setNumRowGroup(int numRowGroup)
    {
        this.numRowGroup = numRowGroup;
    }

    public long getPathId()
    {
        return pathId;
    }

    public void setPathId(long pathId)
    {
        this.pathId = pathId;
    }

    public static List<File> convertFiles(List<MetadataProto.File> protoFiles)
    {
        requireNonNull(protoFiles, "protoFiles is null");
        ImmutableList.Builder<File> filesBuilder =
                ImmutableList.builderWithExpectedSize(protoFiles.size());
        for (MetadataProto.File protoFile : protoFiles)
        {
            filesBuilder.add(new File(protoFile));
        }
        return filesBuilder.build();
    }

    public static List<MetadataProto.File> revertFiles(List<File> files)
    {
        requireNonNull(files, "files is null");
        ImmutableList.Builder<MetadataProto.File> filesBuilder =
                ImmutableList.builderWithExpectedSize(files.size());
        for (File file : files)
        {
            filesBuilder.add(file.toProto());
        }
        return filesBuilder.build();
    }

    @Override
    public MetadataProto.File toProto()
    {
        MetadataProto.File.Builder builder = MetadataProto.File.newBuilder()
                .setId(this.getId()).setName(this.name).setNumRowGroup(this.numRowGroup);
        return builder.setPathId(this.pathId).build();
    }
}
