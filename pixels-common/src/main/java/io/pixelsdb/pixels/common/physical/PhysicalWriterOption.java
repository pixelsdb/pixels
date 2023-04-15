/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.common.physical;

/**
 * @author hank
 * @create 2023-04-15
 */
public class PhysicalWriterOption
{
    // Add more physical writer options here.
    private long blockSize;
    private short replication;
    private boolean addBlockPadding;
    private boolean overwrite;

    public PhysicalWriterOption()
    {
    }

    public long getBlockSize()
    {
        return blockSize;
    }

    public void setBlockSize(long blockSize)
    {
        this.blockSize = blockSize;
    }

    public short getReplication()
    {
        return replication;
    }

    public void setReplication(short replication)
    {
        this.replication = replication;
    }

    public boolean isAddBlockPadding()
    {
        return addBlockPadding;
    }

    public void setAddBlockPadding(boolean addBlockPadding)
    {
        this.addBlockPadding = addBlockPadding;
    }

    public boolean isOverwrite()
    {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite)
    {
        this.overwrite = overwrite;
    }
}
