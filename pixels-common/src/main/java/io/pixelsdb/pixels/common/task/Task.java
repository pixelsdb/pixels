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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-07-26
 */
public class Task
{
    public enum Status
    {
        PENDING, RUNNING, TIMEOUT, COMPLETE, ABORT, FAILED
    }

    private final Integer taskId;
    private final String payload;
    private Status status;
    private Worker<? extends WorkerInfo> worker;

    public Task(int taskId, String payload)
    {
        this.taskId = taskId;
        this.payload = payload;
        this.status = Status.PENDING;
        this.worker = null;
    }

    protected boolean start(Worker<? extends WorkerInfo> worker)
    {
        requireNonNull(worker, "worker is null");
        synchronized (this.taskId)
        {
            if (this.status != Status.PENDING)
            {
                return false;
            }
            if (this.worker != null)
            {
                return false;
            }
            checkArgument(worker.isAlive(), "the worker does not have a valid lease");
            this.worker = worker;
            this.status = Status.RUNNING;
            return true;
        }
    }

    protected boolean complete(boolean success)
    {
        synchronized (this.taskId)
        {
            if (!isRunningWell())
            {
                return false;
            }
            if (success)
            {
                this.status = Status.COMPLETE;
            }
            else
            {
                this.status = Status.FAILED;
            }
            return true;
        }
    }

    protected boolean abort()
    {
        synchronized (this.taskId)
        {
            if (!isRunningWell())
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

    public boolean isRunningWell()
    {
        if (this.status != Status.RUNNING)
        {
            return false;
        }
        if (this.worker == null)
        {
            return false;
        }
        if (!this.worker.isAlive())
        {
            this.status = Status.TIMEOUT;
            return false;
        }
        return true;
    }

    public int getTaskId()
    {
        return taskId;
    }

    public String getPayload()
    {
        return payload;
    }

    @Override
    public int hashCode()
    {
        return 31 * Objects.hashCode(this.taskId) + Objects.hashCode(this.payload);
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
        Task that = (Task) obj;
        return Objects.equals(this.taskId, that.taskId) && Objects.equals(this.payload, that.payload);
    }

    /**
     * @return the json string of this task, <b>DO NOT</b> use this as the payload of task execution requests
     */
    @Override
    public String toString()
    {
        return JSON.toJSONString(this);
    }
}
