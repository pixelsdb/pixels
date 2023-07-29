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
    private final Lease lease;
    private Status status;

    public Task(String id, T payload, long leasePeriodMs)
    {
        this.id = id;
        this.payload = payload;
        this.lease = new Lease(leasePeriodMs);
        this.status = Status.PENDING;
    }

    public Task(String id, String payloadJson, Class<T> clazz, long leasePeriodMs)
    {
        this.id = id;
        this.payload = JSON.parseObject(payloadJson, clazz);
        this.lease = new Lease(leasePeriodMs);
        this.status = Status.PENDING;
    }

    public boolean start(Leaseholder leaseholder)
    {
        synchronized (this.id)
        {
            if (this.status != Status.PENDING)
            {
                return false;
            }
            this.lease.setHolder(requireNonNull(leaseholder, "leaseholder is null"));
            this.lease.setStartTimeMs(System.currentTimeMillis());
            this.status = Status.RUNNING;
            return true;
        }
    }

    public boolean extendLease(Leaseholder leaseholder)
    {
        synchronized (this.id)
        {
            long currentTimeMs = System.currentTimeMillis();
            if (!isRunningWell(leaseholder, currentTimeMs))
            {
                return false;
            }
            this.lease.setStartTimeMs(currentTimeMs);
            return true;
        }
    }

    public Lease getLease()
    {
        return lease;
    }

    public boolean complete(Leaseholder leaseholder)
    {
        synchronized (this.id)
        {
            long currentTimeMs = System.currentTimeMillis();
            if (!isRunningWell(leaseholder, currentTimeMs))
            {
                return false;
            }
            this.status = Status.COMPLETE;
            return true;
        }
    }

    public boolean abort(Leaseholder leaseholder)
    {
        synchronized (this.id)
        {
            long currentTimeMs = System.currentTimeMillis();
            if (!isRunningWell(leaseholder, currentTimeMs))
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

    private boolean isRunningWell(Leaseholder leaseholder, long currentTimeMs)
    {
        if (this.status != Status.RUNNING)
        {
            return false;
        }
        if (!this.lease.isHoldBy(leaseholder))
        {
            return false;
        }
        if (this.lease.hasExpired(currentTimeMs))
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
