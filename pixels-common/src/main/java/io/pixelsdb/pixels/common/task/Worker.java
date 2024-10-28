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
import io.pixelsdb.pixels.common.exception.WorkerCoordinateException;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-07-29
 */
public class Worker<WI extends WorkerInfo>
{
    private final long workerId;
    private int workerPortIndex;
    private final Lease lease;
    private final WI workerInfo;
    private boolean terminated;

    public Worker(long workerId, Lease lease, int workerPortIndex, WI workerInfo)
    {
        this.workerId = workerId;
        this.workerPortIndex = workerPortIndex;
        this.lease = requireNonNull(lease, "lease is null");
        this.workerInfo = requireNonNull(workerInfo, "worker info is null");
        this.terminated = false;
    }

    public long getWorkerId()
    {
        return workerId;
    }

    public void setWorkerPortIndex(int index) { this.workerPortIndex = index; }

    public int getWorkerPortIndex() { return workerPortIndex; }

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
            if (this.terminated)
            {
                return false;
            }
            long currentTimeMs = System.currentTimeMillis();
            return !this.lease.hasExpired(currentTimeMs);
        }
    }

    public void terminate()
    {
        synchronized (this.lease)
        {
            this.terminated = true;
        }
    }

    /**
     * Extent the lease of this worker if the worker is alive.
     * @return the new start time (milliseconds since the Unix epoch) of the extended lease
     * @throws WorkerCoordinateException if the worker is terminated or the lease has already expired
     */
    public long extendLease() throws WorkerCoordinateException
    {
        synchronized (this.lease)
        {
            long currentTimeMs = System.currentTimeMillis();
            if (this.terminated || this.lease.hasExpired(currentTimeMs))
            {
                throw new WorkerCoordinateException("worker is not alive, can not extend the lease");
            }
            this.lease.setStartTimeMs(currentTimeMs);
            return currentTimeMs;
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
