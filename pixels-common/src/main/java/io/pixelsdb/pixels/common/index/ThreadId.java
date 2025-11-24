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
 * @author hank
 * @create 2025-11-25
 */
public class ThreadId implements Comparable<ThreadId>
{
    private final long threadId;

    public ThreadId()
    {
        this.threadId = Thread.currentThread().getId();
    }

    @Override
    public boolean equals(Object obj)
    {
        ThreadId that = (ThreadId) obj;
        return this.threadId == that.threadId;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(threadId);
    }

    @Override
    public int compareTo(ThreadId that)
    {
        return Long.compare(this.threadId, that.threadId);
    }
}
