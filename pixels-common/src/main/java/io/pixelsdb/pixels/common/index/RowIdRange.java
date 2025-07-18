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

import java.util.Objects;

/**
 * A range of continuous row ids in a file and row group. When writing data at high throughput,
 * we get a range of row ids at a time from the index cache, and insert it into the persistent main index storage.
 *
 * @author hank
 * @create 2025-02-19
 */
public class RowIdRange implements Comparable<RowIdRange>
{
    // rowIdStart and rowIdEnd are the keys
    /**
     * inclusive
     */
    private final long rowIdStart;
    /**
     * exclusive
     */
    private long rowIdEnd;
    // the following fields are the payloads
    private final long fileId;
    private final int rgId;
    /**
     * inclusive
     */
    private final int rgRowIdStart;
    /**
     * exclusive
     */
    private int rgRowIdEnd;

    public RowIdRange(long rowIdStart, long rowIdEnd, long fileId, int rgId, int rgRowIdStart, int rgRowIdEnd)
    {
        this.rowIdStart = rowIdStart;
        this.rowIdEnd = rowIdEnd;
        this.fileId = fileId;
        this.rgId = rgId;
        this.rgRowIdStart = rgRowIdStart;
        this.rgRowIdEnd = rgRowIdEnd;
    }

    public RowIdRange(long rowIdStart, long fileId, int rgId, int rgRowIdStart)
    {
        this.rowIdStart = rowIdStart;
        this.fileId = fileId;
        this.rgId = rgId;
        this.rgRowIdStart = rgRowIdStart;
    }

    public long getRowIdStart()
    {
        return rowIdStart;
    }

    public long getRowIdEnd()
    {
        return rowIdEnd;
    }

    public long getFileId()
    {
        return fileId;
    }

    public int getRgId()
    {
        return rgId;
    }

    public int getRgRowIdStart()
    {
        return rgRowIdStart;
    }

    public int getRgRowIdEnd()
    {
        return rgRowIdEnd;
    }

    public void setRowIdEnd(long rowIdEnd)
    {
        this.rowIdEnd = rowIdEnd;
    }

    public void setRgRowIdEnd(int rgRowIdEnd)
    {
        this.rgRowIdEnd = rgRowIdEnd;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof RowIdRange)) return false;
        RowIdRange that = (RowIdRange) o;
        return rowIdStart == that.rowIdStart && rowIdEnd == that.rowIdEnd;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowIdStart, rowIdEnd);
    }

    @Override
    public int compareTo(RowIdRange o)
    {
        if (this.rowIdStart != o.rowIdStart)
        {
            return this.rowIdStart < o.rowIdStart ? -1 : 1;
        }
        return this.rowIdEnd < o.rowIdEnd ? -1 : 1;
    }
}
