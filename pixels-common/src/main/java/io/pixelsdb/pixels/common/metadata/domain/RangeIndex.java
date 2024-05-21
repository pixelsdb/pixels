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

import java.nio.ByteBuffer;

/**
 * @author hank
 * @create 2024-05-22
 */
public class RangeIndex
{
    private ByteBuffer struct;
    private KeyColumns keyColumns;
    private String keyColumnsJson;
    private long tableId;

    public RangeIndex()
    {
    }

    public ByteBuffer getStruct()
    {
        return struct;
    }

    public void setStruct(ByteBuffer struct)
    {
        this.struct = struct;
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
}
