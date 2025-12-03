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
 */
public class Lease
{
    private final long periodMs;
    private final AtomicLong startTimeMs;

    public Lease(long periodMs, long startTimeMs)
    {
        this.periodMs = periodMs;
        this.startTimeMs = new AtomicLong(startTimeMs);
    }

    public long getPeriodMs()
    {
        return periodMs;
    }

    public long getStartTimeMs()
    {
        return startTimeMs.get();
    }

    /**
     * Update the start times of the lease. The new start time must be larger than the current start time of the lease.
     * @param newStartTimeMs the new start time
     * @return true if the new start time is set successfully, false if it is not larger than the current start time
     */
    public boolean updateStartTimeMs(long newStartTimeMs)
    {
        long currStartTimeMs;
        do
        {
            currStartTimeMs = this.startTimeMs.get();
            if (newStartTimeMs <= currStartTimeMs)
            {
                return false;
            }
        }
        while (!this.startTimeMs.compareAndSet(currStartTimeMs, newStartTimeMs));
        return true;
    }

    public boolean hasExpired(long currentTimeMs, Role role)
    {
        if (role == Role.Assigner)
        {
            return currentTimeMs - this.startTimeMs.get() > this.periodMs;
        }
        else
        {
            return currentTimeMs - this.startTimeMs.get() - LEASE_TIME_SKEW_MS - LEASE_NETWORK_LATENCY_MS > this.periodMs;
        }
    }

    public enum Role
    {
        Assigner, // the role that assigns the lease
        Applier // the role that applies and holds the lease
    }
}
