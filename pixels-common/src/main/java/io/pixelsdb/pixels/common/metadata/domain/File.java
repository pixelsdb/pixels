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
import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2024-06-08
 */
public class File extends Base
{
    public enum Type
    {
        TEMPORARY_INGEST(0),
        REGULAR(1),
        TEMPORARY_GC(2),
        RETIRED(3);

        private final int number;

        Type(int number)
        {
            this.number = number;
        }

        public int getNumber()
        {
            return number;
        }

        public static Type valueOf(int number)
        {
            switch (number)
            {
                case 0:
                    return TEMPORARY_INGEST;
                case 1:
                    return REGULAR;
                case 2:
                    return TEMPORARY_GC;
                case 3:
                    return RETIRED;
                default:
                    throw new InvalidArgumentException("invalid number for File.Type");
            }
        }
    }

    private String name;
    private Type type;
    private int numRowGroup;
    private long minRowId;
    private long maxRowId;
    private long pathId;
    private Long cleanupAt;

    public File()
    {
    }

    public File(MetadataProto.File file)
    {
        this.setId(file.getId());
        this.name = file.getName();
        this.type = Type.valueOf(file.getTypeValue());
        this.numRowGroup = file.getNumRowGroup();
        this.minRowId = file.getMinRowId();
        this.maxRowId = file.getMaxRowId();
        this.pathId = file.getPathId();
        this.cleanupAt = file.hasCleanupAt() ? file.getCleanupAt() : null;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Type getType()
    {
        return type;
    }

    public void setType(Type type)
    {
        this.type = type;
    }

    public int getNumRowGroup()
    {
        return numRowGroup;
    }

    public void setNumRowGroup(int numRowGroup)
    {
        this.numRowGroup = numRowGroup;
    }

    public long getMinRowId()
    {
        return minRowId;
    }

    public void setMinRowId(long minRowId)
    {
        this.minRowId = minRowId;
    }

    public long getMaxRowId()
    {
        return maxRowId;
    }

    public void setMaxRowId(long maxRowId)
    {
        this.maxRowId = maxRowId;
    }

    public long getPathId()
    {
        return pathId;
    }

    public void setPathId(long pathId)
    {
        this.pathId = pathId;
    }

    public Long getCleanupAt()
    {
        return cleanupAt;
    }

    public void setCleanupAt(Long cleanupAt)
    {
        this.cleanupAt = cleanupAt;
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

    /**
     * @param path the path containing the file
     * @param file the file
     * @return the full absolute path of the file
     */
    public static String getFilePath(Path path, File file)
    {
        String pathUri = path.getUri();
        if (pathUri.endsWith("/"))
        {
            return pathUri + file.name;
        }
        else
        {
            return pathUri + "/" + file.name;
        }
    }

    @Override
    public MetadataProto.File toProto()
    {
        MetadataProto.File.Builder builder = MetadataProto.File.newBuilder()
                .setId(this.getId()).setName(this.name)
                .setTypeValue(this.type.getNumber()).setNumRowGroup(this.numRowGroup)
                .setMinRowId(this.minRowId).setMaxRowId(this.maxRowId).setPathId(this.pathId);
        if (this.cleanupAt != null)
        {
            builder.setCleanupAt(this.cleanupAt);
        }
        return builder.build();
    }
}
