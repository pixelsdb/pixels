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
    private final long rowIdEnd;
    // the following fields are the payloads
    private final long fileId;
    private final int rgId;
    /**
     * inclusive
     */
    private final int rgRowOffsetStart;
    /**
     * exclusive
     */
    private final int rgRowOffsetEnd;

    public RowIdRange(long rowIdStart, long rowIdEnd, long fileId, int rgId, int rgRowOffsetStart, int rgRowOffsetEnd)
    {
        this.rowIdStart = rowIdStart;
        this.rowIdEnd = rowIdEnd;
        this.fileId = fileId;
        this.rgId = rgId;
        this.rgRowOffsetStart = rgRowOffsetStart;
        this.rgRowOffsetEnd = rgRowOffsetEnd;
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

    public int getRgRowOffsetStart()
    {
        return rgRowOffsetStart;
    }

    public int getRgRowOffsetEnd()
    {
        return rgRowOffsetEnd;
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

    public Builder toBuilder ()
    {
        return new Builder(this);
    }

    public static class Builder
    {
        /**
         * inclusive
         */
        private long rowIdStart;
        /**
         * exclusive
         */
        private long rowIdEnd;
        // the following fields are the payloads
        private long fileId;
        private int rgId;
        /**
         * inclusive
         */
        private int rgRowOffsetStart;
        /**
         * exclusive
         */
        private int rgRowOffsetEnd;

        public Builder () { }

        public Builder (RowIdRange rowIdRange)
        {
            this.rowIdStart = rowIdRange.rowIdStart;
            this.rowIdEnd = rowIdRange.rowIdEnd;
            this.fileId = rowIdRange.fileId;
            this.rgId = rowIdRange.rgId;
            this.rgRowOffsetStart = rowIdRange.rgRowOffsetStart;
            this.rgRowOffsetEnd = rowIdRange.rgRowOffsetEnd;
        }

        public long getRowIdStart()
        {
            return rowIdStart;
        }

        public Builder setRowIdStart(long rowIdStart)
        {
            this.rowIdStart = rowIdStart;
            return this;
        }

        public long getRowIdEnd()
        {
            return rowIdEnd;
        }

        public Builder setRowIdEnd(long rowIdEnd)
        {
            this.rowIdEnd = rowIdEnd;
            return this;
        }

        public long getFileId()
        {
            return fileId;
        }

        public Builder setFileId(long fileId)
        {
            this.fileId = fileId;
            return this;
        }

        public int getRgId()
        {
            return rgId;
        }

        public Builder setRgId(int rgId)
        {
            this.rgId = rgId;
            return this;
        }

        public int getRgRowOffsetStart()
        {
            return rgRowOffsetStart;
        }

        public Builder setRgRowOffsetStart(int rgRowOffsetStart)
        {
            this.rgRowOffsetStart = rgRowOffsetStart;
            return this;
        }

        public int getRgRowOffsetEnd()
        {
            return rgRowOffsetEnd;
        }

        public Builder setRgRowOffsetEnd(int rgRowOffsetEnd)
        {
            this.rgRowOffsetEnd = rgRowOffsetEnd;
            return this;
        }

        public RowIdRange build ()
        {
            return new RowIdRange(this.rowIdStart, this.rowIdEnd,
                    this.fileId, this.rgId, this.rgRowOffsetStart, this.rgRowOffsetEnd);
        }
    }
}
