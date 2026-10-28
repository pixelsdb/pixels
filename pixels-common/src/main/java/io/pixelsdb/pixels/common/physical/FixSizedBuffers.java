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
 * @since 2025-09-22
 */
public class FixSizedBuffers
{
    private final int bufferSize;
    private final int maxCached;
    private int cached;

    private final Queue<byte[]> freeBuffers = new ConcurrentLinkedQueue<>();

    public FixSizedBuffers(int bufferSize)
    {
        this(bufferSize, Integer.MAX_VALUE);
    }

    public FixSizedBuffers(int bufferSize, int maxCached)
    {
        checkArgument(bufferSize > 0, "bufferSize must be positive");
        checkArgument(maxCached >= 0, "maxCached must be non-negative");
        this.bufferSize = bufferSize;
        this.maxCached = maxCached;
    }

    public byte[] allocate()
    {
        byte[] buffer;
        synchronized (freeBuffers)
        {
            buffer = this.freeBuffers.poll();
            if (buffer != null) { cached--; }
        }
        if (buffer == null)
        {
            buffer = new byte[bufferSize];
        }
        return buffer;
    }

    public void free(@Nonnull byte[] buffer)
    {
        requireNonNull(buffer, "buffer cannot be null");
        checkArgument(buffer.length == this.bufferSize,
                "buffer length does not match expected size");
        synchronized (freeBuffers)
        {
            if (cached < maxCached) { this.freeBuffers.offer(buffer); cached++; }
        }
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public int getCachedBufferCount() { synchronized (freeBuffers) { return cached; } }

    public void clear()
    {
        synchronized (freeBuffers) { this.freeBuffers.clear(); cached = 0; }
    }
}
