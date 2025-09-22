/*
 * Copyright 2021-2023 PixelsDB.
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

import javax.annotation.Nonnull;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2025-09-22
 */
public class FixSizedBuffers
{
    private final int bufferSize;

    private final Queue<byte[]> freeBuffers = new ConcurrentLinkedQueue<>();

    public FixSizedBuffers(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    public byte[] allocate()
    {
        byte[] buffer = this.freeBuffers.poll();
        if (buffer == null)
        {
            buffer = new byte[bufferSize];
            this.freeBuffers.add(buffer);
        }
        return buffer;
    }

    public void free(@Nonnull byte[] buffer)
    {
        requireNonNull(buffer, "buffer cannot be null");
        checkArgument(buffer.length == this.bufferSize,
                "buffer length does not match expected size");
        this.freeBuffers.add(buffer);
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public void clear()
    {
        this.freeBuffers.clear();
    }
}
