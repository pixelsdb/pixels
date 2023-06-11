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

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.daemon.MetadataProto;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 */
public class Column extends Base
{
    private String name;
    private String type;
    private double chunkSize;
    private double size;
    private double nullFraction;
    private long cardinality;
    private ByteBuffer recordStats;
    private long tableId;

    public Column()
    {
    }

    public Column(MetadataProto.Column column)
    {
        this.setId(column.getId());
        this.name = column.getName();
        this.type = column.getType();
        this.chunkSize = column.getChunkSize();
        this.size = column.getSize();
        this.nullFraction = column.getNullFraction();
        this.cardinality = column.getCardinality();
        // The default value of bytes in protobuf is empty bytes, thus no need to check for null.
        this.recordStats = column.getRecordStats().asReadOnlyByteBuffer();
        this.tableId = column.getTableId();
    }

    public static List<Column> convertColumns(List<MetadataProto.Column> protoColumns)
    {
        requireNonNull(protoColumns, "protoColumns is null");
        ImmutableList.Builder<Column> columnsBuilder =
                ImmutableList.builderWithExpectedSize(protoColumns.size());
        for (MetadataProto.Column protoColumn : protoColumns)
        {
            columnsBuilder.add(new Column(protoColumn));
        }
        return columnsBuilder.build();
    }

    public static List<MetadataProto.Column> revertColumns(List<Column> columns)
    {
        requireNonNull(columns, "columns is null");
        ImmutableList.Builder<MetadataProto.Column> columnsBuilder =
                ImmutableList.builderWithExpectedSize(columns.size());
        for (Column column : columns)
        {
            columnsBuilder.add(column.toProto());
        }
        return columnsBuilder.build();
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Get the full type's display name, e.g., integer, varchar(10).
     * @return
     */
    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    @JSONField(serialize = false)
    public double getChunkSize()
    {
        return chunkSize;
    }

    public void setChunkSize(double chunkSize)
    {
        this.chunkSize = chunkSize;
    }

    @JSONField(serialize = false)
    public double getSize()
    {
        return size;
    }

    public void setSize(double size)
    {
        this.size = size;
    }

    @JSONField(serialize = false)
    public double getNullFraction()
    {
        return nullFraction;
    }

    @JSONField(serialize = false)
    public void setNullFraction(double nullFraction)
    {
        this.nullFraction = nullFraction;
    }

    @JSONField(serialize = false)
    public long getCardinality()
    {
        return cardinality;
    }

    public void setCardinality(long cardinality)
    {
        this.cardinality = cardinality;
    }

    @JSONField(serialize = false)
    public ByteBuffer getRecordStats()
    {
        return recordStats;
    }

    public void setRecordStats(ByteBuffer recordStats)
    {
        this.recordStats = recordStats;
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
        return "Column{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", chunkSize=" + chunkSize +
                ", size=" + size +
                ", nullFraction=" + nullFraction +
                ", cardinality=" + cardinality +
                ", recordStats=bytes(" + recordStats.remaining() +
                "), tableId=" + tableId +
                '}';
    }

    private MetadataProto.Column toProto()
    {
        return MetadataProto.Column.newBuilder().setId(this.getId()).setName(this.name).setType(this.type)
                .setChunkSize(this.chunkSize).setSize(this.size).setNullFraction(this.nullFraction)
                .setCardinality(this.cardinality).setRecordStats(ByteString.copyFrom(this.recordStats))
                .setTableId(this.tableId).build();
    }
}
