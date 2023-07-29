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
package io.pixelsdb.pixels.common.task;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2023-07-29
 */
public class Lease
{
    private final long periodMs;
    private long startTimeMs;
    private LeaseHolder owner;

    public Lease(long periodMs)
    {
        this.periodMs = periodMs;
        this.startTimeMs = 0L;
        this.owner = null;
    }

    public long getPeriodMs()
    {
        return periodMs;
    }

    public long getStartTimeMs()
    {
        return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs)
    {
        checkArgument(startTimeMs > this.startTimeMs,
                "the new start time must be later than the current one");
        this.startTimeMs = startTimeMs;
    }

    public LeaseHolder getOwner()
    {
        return owner;
    }

    public void setOwner(LeaseHolder owner)
    {
        this.owner = owner;
    }

    public boolean isHoldBy(LeaseHolder leaseHolder)
    {
        if (this.owner == null || leaseHolder == null)
        {
            return false;
        }
        return this.owner == leaseHolder || this.owner.equals(leaseHolder);
    }

    public boolean hasExpired(long currentTimeMs)
    {
        return currentTimeMs - this.startTimeMs > this.periodMs;
    }
}
