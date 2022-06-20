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

import java.nio.ByteBuffer;

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
    private ByteBuffer minValue;
    private ByteBuffer maxValue;
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
        this.minValue = column.getMinValue().asReadOnlyByteBuffer();
        this.maxValue = column.getMaxValue().asReadOnlyByteBuffer();
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

    public double getChunkSize()
    {
        return chunkSize;
    }

    public void setChunkSize(double chunkSize)
    {
        this.chunkSize = chunkSize;
    }

    public double getSize()
    {
        return size;
    }

    public void setSize(double size)
    {
        this.size = size;
    }

    public double getNullFraction()
    {
        return nullFraction;
    }

    public void setNullFraction(double nullFraction)
    {
        this.nullFraction = nullFraction;
    }

    public long getCardinality()
    {
        return cardinality;
    }

    public void setCardinality(long cardinality)
    {
        this.cardinality = cardinality;
    }

    public ByteBuffer getMinValue()
    {
        return minValue;
    }

    public void setMinValue(ByteBuffer minValue)
    {
        this.minValue = minValue;
    }

    public ByteBuffer getMaxValue()
    {
        return maxValue;
    }

    public void setMaxValue(ByteBuffer maxValue)
    {
        this.maxValue = maxValue;
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
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                ", tableId=" + tableId +
                '}';
    }
}
