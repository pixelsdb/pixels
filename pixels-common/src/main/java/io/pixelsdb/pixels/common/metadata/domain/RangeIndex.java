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

import com.alibaba.fastjson.JSON;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.daemon.MetadataProto;

import java.nio.ByteBuffer;

/**
 * @author hank
 * @create 2024-05-22
 */
public class RangeIndex extends Base
{
    private ByteBuffer indexStruct;
    private KeyColumns keyColumns;
    private String keyColumnsJson;
    private long tableId;

    public RangeIndex()
    {
    }

    public RangeIndex(MetadataProto.RangeIndex rangeIndex)
    {
        this.setId(rangeIndex.getId());
        this.indexStruct = rangeIndex.getIndexStruct().asReadOnlyByteBuffer();
        this.keyColumnsJson = rangeIndex.getKeyColumns();
        this.keyColumns = JSON.parseObject(this.keyColumnsJson, KeyColumns.class);
        this.tableId = rangeIndex.getTableId();
    }

    public ByteBuffer getIndexStruct()
    {
        return indexStruct;
    }

    public void setIndexStruct(ByteBuffer indexStruct)
    {
        this.indexStruct = indexStruct;
    }

    public KeyColumns getKeyColumns()
    {
        return keyColumns;
    }

    public void setKeyColumns(KeyColumns keyColumns)
    {
        this.keyColumns = keyColumns;
    }

    public String getKeyColumnsJson()
    {
        return keyColumnsJson;
    }

    public void setKeyColumnsJson(String keyColumnsJson)
    {
        this.keyColumnsJson = keyColumnsJson;
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
    public MetadataProto.RangeIndex toProto()
    {
        return MetadataProto.RangeIndex.newBuilder().setId(this.getId())
                .setIndexStruct(ByteString.copyFrom(this.indexStruct)).setKeyColumns(this.keyColumnsJson)
                .setTableId(this.tableId).build();
    }
}
