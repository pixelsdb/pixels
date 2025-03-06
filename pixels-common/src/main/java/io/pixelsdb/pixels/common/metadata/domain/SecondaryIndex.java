/*
 * Copyright 2025 PixelsDB.
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
import io.pixelsdb.pixels.common.index.SecondaryIndex.Scheme;
import io.pixelsdb.pixels.daemon.MetadataProto;

/**
 * @author hank
 * @create 2025-02-07
 */
public class SecondaryIndex extends Base
{
    private KeyColumns keyColumns;
    private String keyColumnsJson;
    private boolean unique;
    private Scheme indexScheme;
    private long tableId;

    public SecondaryIndex()
    {
    }

    public SecondaryIndex(MetadataProto.SecondaryIndex secondaryIndex)
    {
        this.setId(secondaryIndex.getId());
        this.keyColumnsJson = secondaryIndex.getKeyColumns();
        this.keyColumns = JSON.parseObject(this.keyColumnsJson, KeyColumns.class);
        this.unique = secondaryIndex.getUnique();
        this.indexScheme = Scheme.from(secondaryIndex.getIndexScheme());
        this.tableId = secondaryIndex.getTableId();
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

    public boolean isUnique()
    {
        return unique;
    }

    public void setUnique(boolean unique)
    {
        this.unique = unique;
    }

    public Scheme getIndexScheme()
    {
        return indexScheme;
    }

    public void setIndexScheme(Scheme indexScheme)
    {
        this.indexScheme = indexScheme;
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
    public MetadataProto.SecondaryIndex toProto()
    {
        return MetadataProto.SecondaryIndex.newBuilder().setId(this.getId()).setKeyColumns(this.keyColumnsJson)
                .setUnique(this.unique).setIndexScheme(this.indexScheme.name()).setTableId(this.tableId).build();
    }
}
