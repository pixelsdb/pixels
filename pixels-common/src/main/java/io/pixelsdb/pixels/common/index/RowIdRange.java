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
package io.pixelsdb.pixels.common.index;

/**
 * A range of continuous row ids. When writing data at high throughput,
 * we get a range of row ids at a time from the index server, thus we can reduce the rpc overhead.
 *
 * @author hank
 * @create 2025-02-19
 */
public class RowIdRange
{
    private final long startRowId;
    private final long endRowId;

    public RowIdRange(long startRowId, long endRowId)
    {
        this.startRowId = startRowId;
        this.endRowId = endRowId;
    }

    public long getStartRowId()
    {
        return startRowId;
    }

    public long getEndRowId()
    {
        return endRowId;
    }
}
