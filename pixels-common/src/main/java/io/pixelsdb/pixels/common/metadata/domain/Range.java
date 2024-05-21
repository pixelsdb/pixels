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
public class Range
{
    private ByteBuffer min;
    private ByteBuffer max;
    private long parentId;
    private long rangeIndexId;

    public Range()
    {
    }

    public ByteBuffer getMin()
    {
        return min;
    }

    public void setMin(ByteBuffer min)
    {
        this.min = min;
    }

    public ByteBuffer getMax()
    {
        return max;
    }

    public void setMax(ByteBuffer max)
    {
        this.max = max;
    }

    public long getParentId()
    {
        return parentId;
    }

    public void setParentId(long parentId)
    {
        this.parentId = parentId;
    }

    public long getRangeIndexId()
    {
        return rangeIndexId;
    }

    public void setRangeIndexId(long rangeIndexId)
    {
        this.rangeIndexId = rangeIndexId;
    }
}
