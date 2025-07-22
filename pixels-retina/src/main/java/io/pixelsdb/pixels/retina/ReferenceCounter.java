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
package io.pixelsdb.pixels.retina;

import java.util.concurrent.atomic.AtomicInteger;

public class ReferenceCounter
{
    private final AtomicInteger count = new AtomicInteger(0);

    /**
     * Increment the reference count.
     */
    public void ref()
    {
        count.incrementAndGet();
    }

    /**
     * Decrement the reference count and check whether it is zero.
     * If zero, release internal resources, the object itself releases
     * resources called by the caller.
     */
    public boolean unref()
    {
        int newCount = count.decrementAndGet();
        assert  newCount >= 0 : "Reference count should not be negative";
        return newCount == 0;
    }

    /**
     * Get the current reference count.
     */
    public int getCount()
    {
        return count.get();
    }
}
