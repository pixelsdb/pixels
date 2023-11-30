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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.turbo.TurboProto;

/**
 * @author hank
 * @create 2023-10-04
 */
public class TaskInfo
{
    private final int taskId;
    private final String payload;
    private boolean success;

    public TaskInfo(int taskId, String payload)
    {
        this.taskId = taskId;
        this.payload = payload;
    }

    public TaskInfo(TurboProto.TaskInput taskInput)
    {
        this.taskId = taskInput.getTaskId();
        this.payload = taskInput.getPayload();
    }

    public int getTaskId()
    {
        return taskId;
    }

    public String getPayload()
    {
        return payload;
    }

    public boolean isSuccess()
    {
        return success;
    }

    public void setSuccess(boolean success)
    {
        this.success = success;
    }

    public TurboProto.TaskResult toTaskResultProto()
    {
        return TurboProto.TaskResult.newBuilder().setTaskId(this.taskId).setSuccess(this.success).build();
    }
}
