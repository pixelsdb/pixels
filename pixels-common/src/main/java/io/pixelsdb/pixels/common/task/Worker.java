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

import com.alibaba.fastjson.JSON;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-07-29
 */
public class Worker<WI extends WorkerInfo>
{
    private final long workerId;
    private final Lease lease;
    private final WI workerInfo;

    public Worker(long workerId, Lease lease, WI workerInfo)
    {
        this.workerId = workerId;
        this.lease = requireNonNull(lease, "lease is null");
        this.workerInfo = requireNonNull(workerInfo, "worker info is null");
    }

    public long getWorkerId()
    {
        return workerId;
    }

    public WI getWorkerInfo()
    {
        return workerInfo;
    }

    /**
     * @return true if this worker has a valid lease
     */
    public boolean isAlive()
    {
        synchronized (this.lease)
        {
            long currentTimeMs = System.currentTimeMillis();
            return !this.lease.hasExpired(currentTimeMs);
        }
    }

    /**
     * Extent the lease of this worker if the lease is not expired.
     * @return true if the lease is extended successfully, false if the lease has expired
     */
    public boolean extendLease()
    {
        synchronized (this.lease)
        {
            long currentTimeMs = System.currentTimeMillis();
            if (this.lease.hasExpired(currentTimeMs))
            {
                return false;
            }
            this.lease.setStartTimeMs(currentTimeMs);
            return true;
        }
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(this.workerId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof Worker))
        {
            return false;
        }
        Worker that = (Worker) obj;
        return this.workerId == that.workerId;
    }

    @Override
    public String toString()
    {
        return JSON.toJSONString(this);
    }
}
