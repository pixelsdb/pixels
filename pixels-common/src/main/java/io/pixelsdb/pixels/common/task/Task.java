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
package io.pixelsdb.pixels.common.task;

import com.alibaba.fastjson.JSON;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-07-26
 */
public class Task<T>
{
    public enum Status
    {
        PENDING, RUNNING, TIMEOUT, COMPLETE, ABORT
    }

    private final String id;
    private final T payload;
    private final long leasePeriodMs;
    private Status status;
    private long leaseStartTimeMs;
    private LeaseOwner leaseOwner;

    public Task(String id, T payload, long leasePeriodMs)
    {
        this.id = id;
        this.payload = payload;
        this.leasePeriodMs = leasePeriodMs;
        this.status = Status.PENDING;
        this.leaseStartTimeMs = 0;
        this.leaseOwner = null;
    }

    public Task(String id, String payloadJson, Class<T> clazz, long leasePeriodMs)
    {
        this.id = id;
        this.payload = JSON.parseObject(payloadJson, clazz);
        this.leasePeriodMs = leasePeriodMs;
        this.status = Status.PENDING;
        this.leaseStartTimeMs = 0;
        this.leaseOwner = null;
    }

    public boolean start(LeaseOwner leaseOwner)
    {
        synchronized (this.id)
        {
            if (this.status != Status.PENDING)
            {
                return false;
            }
            this.leaseOwner = requireNonNull(leaseOwner, "lease owner is null");
            this.leaseStartTimeMs = System.currentTimeMillis();
            this.status = Status.RUNNING;
            return true;
        }
    }

    public boolean extendLease(LeaseOwner leaseOwner)
    {
        synchronized (this.id)
        {
            long currentTimeMs = System.currentTimeMillis();
            if (!isRunningWell(leaseOwner, currentTimeMs))
            {
                return false;
            }
            this.leaseStartTimeMs = currentTimeMs;
            return true;
        }
    }

    public boolean complete(LeaseOwner leaseOwner)
    {
        synchronized (this.id)
        {
            long currentTimeMs = System.currentTimeMillis();
            if (!isRunningWell(leaseOwner, currentTimeMs))
            {
                return false;
            }
            this.status = Status.COMPLETE;
            return true;
        }
    }

    public boolean abort(LeaseOwner leaseOwner)
    {
        synchronized (this.id)
        {
            long currentTimeMs = System.currentTimeMillis();
            if (!isRunningWell(leaseOwner, currentTimeMs))
            {
                return false;
            }
            this.status = Status.ABORT;
            return true;
        }
    }

    public Status getStatus()
    {
        return status;
    }

    public boolean isOwnedBy(LeaseOwner leaseOwner)
    {
        if (this.leaseOwner == null || leaseOwner == null)
        {
            return false;
        }
        return this.leaseOwner == leaseOwner || this.leaseOwner.equals(leaseOwner);
    }

    private boolean isRunningWell(LeaseOwner leaseOwner, long currentTimeMs)
    {
        if (this.status != Status.RUNNING)
        {
            return false;
        }
        if (!isOwnedBy(leaseOwner))
        {
            return false;
        }
        if (currentTimeMs - this.leaseStartTimeMs > leasePeriodMs)
        {
            this.status = Status.TIMEOUT;
            return false;
        }
        return true;
    }

    public String getPayloadJson()
    {
        return JSON.toJSONString(this.payload);
    }

    public String getId()
    {
        return id;
    }

    public T getPayload()
    {
        return payload;
    }

    @Override
    public int hashCode()
    {
        return 31 * Objects.hashCode(this.id) + Objects.hashCode(this.payload);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof Task))
        {
            return false;
        }
        Task<?> that = (Task<?>) obj;
        return Objects.equals(this.id, that.id) && Objects.equals(this.payload, that.payload);
    }

    @Override
    public String toString()
    {
        return JSON.toJSONString(this);
    }
}
