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
package io.pixelsdb.pixels.common.lease;

import java.util.concurrent.atomic.AtomicLong;

import static io.pixelsdb.pixels.common.utils.Constants.LEASE_NETWORK_LATENCY_MS;
import static io.pixelsdb.pixels.common.utils.Constants.LEASE_TIME_SKEW_MS;

/**
 * @author hank
 * @create 2023-07-29
 * @update 2025-12-03 make {@link #startMs} atomic and add {@link Role}.
 */
public class Lease
{
    private final long periodMs;
    private final AtomicLong startMs;

    public Lease(long periodMs, long startMs)
    {
        this.periodMs = periodMs;
        this.startMs = new AtomicLong(startMs);
    }

    public long getPeriodMs()
    {
        return periodMs;
    }

    public long getStartMs()
    {
        return startMs.get();
    }

    /**
     * Update the start times of the lease. The new start time must be larger than the current start time of the lease.
     * @param newStartMs the new start time
     * @return true if the new start time is set successfully, false if it is not larger than the current start time
     */
    public boolean updateStartMs(long newStartMs)
    {
        long currStartMs;
        do
        {
            currStartMs = this.startMs.get();
            if (newStartMs <= currStartMs)
            {
                return false;
            }
        }
        while (!this.startMs.compareAndSet(currStartMs, newStartMs));
        return true;
    }

    public boolean hasExpired(long currentTimeMs, Role role)
    {
        if (role == Role.Assigner)
        {
            return currentTimeMs - this.startMs.get() > this.periodMs;
        }
        else
        {
            return currentTimeMs - this.startMs.get() - LEASE_TIME_SKEW_MS - LEASE_NETWORK_LATENCY_MS > this.periodMs;
        }
    }

    public enum Role
    {
        Assigner, // the role that assigns the lease
        Applier // the role that applies and holds the lease
    }
}
