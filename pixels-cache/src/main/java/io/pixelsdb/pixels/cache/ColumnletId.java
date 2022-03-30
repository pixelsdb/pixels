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
package io.pixelsdb.pixels.cache;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * This is used to represent the cached column chunk inside a file.
 * In Pixels, columnlet = column chunk.
 *
 * @author guodong
 * @author hank
 */
public class ColumnletId
{
    public short rowGroupId;
    public short columnId;
    public boolean direct;

    public ColumnletId(short rowGroupId, short columnId, boolean direct)
    {
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
        this.direct = direct;
    }

    public ColumnletId() {}

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ColumnletId other = (ColumnletId) o;
        return Objects.equals(rowGroupId, other.rowGroupId) &&
                Objects.equals(columnId, other.columnId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("row group id", rowGroupId)
                .add("column id", columnId)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowGroupId, columnId);
    }
}
