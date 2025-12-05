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

import io.pixelsdb.pixels.common.exception.InvalidArgumentException;

import java.util.concurrent.atomic.AtomicLong;

import static io.pixelsdb.pixels.common.utils.Constants.*;

/**
 * @author hank
 * @create 2023-07-29
 * @update 2025-12-03 make {@link #startMs} atomic and add {@link Role}.
 */
public class Lease
{
    private final long periodMs;
    private final AtomicLong startMs;

    public Lease(long startMs, long periodMs)
    {
        this.startMs = new AtomicLong(startMs);
        this.periodMs = periodMs;
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

    /**
     * This method can be called by lease assigner and applier.
     * @param currentTimeMs
     * @param role
     * @return
     */
    public boolean hasExpired(long currentTimeMs, Role role)
    {
        if (role == Role.Assigner)
        {
            return currentTimeMs - this.startMs.get() > this.periodMs;
        }
        else if (role == Role.Applier)
        {
            return currentTimeMs - this.startMs.get() > this.periodMs - LEASE_TIME_SKEW_MS - LEASE_NETWORK_LATENCY_MS;
        }
        else
        {
            throw new InvalidArgumentException("invalid lease role " + role.name());
        }
    }

    /**
     * This method can be only called by lease applier.
     * @param currentTimeMs
     * @param role
     * @return
     */
    public boolean expiring(long currentTimeMs, Role role)
    {
        if (role == Role.Applier)
        {
            return currentTimeMs - this.startMs.get() >
                    this.periodMs * LEASE_EXPIRING_THRESHOLD - LEASE_TIME_SKEW_MS - LEASE_NETWORK_LATENCY_MS;
        }
        throw new InvalidArgumentException("mayExpire should only be called by lease applier");
    }

    public enum Role
    {
        Assigner, // the role that assigns the lease
        Applier // the role that applies and holds the lease
    }
}
