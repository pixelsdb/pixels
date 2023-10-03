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
    private final String taskId;
    private final String payload;
    private String output;

    public TaskInfo(String taskId, String payload)
    {
        this.taskId = taskId;
        this.payload = payload;
    }

    public TaskInfo(TurboProto.TaskInput taskInput)
    {
        this.taskId = taskInput.getTaskId();
        this.payload = taskInput.getPayload();
    }

    public String getTaskId()
    {
        return taskId;
    }

    public String getPayload()
    {
        return payload;
    }

    public String getOutput()
    {
        return output;
    }

    public void setOutput(String output)
    {
        this.output = output;
    }

    public TurboProto.TaskOutput toTaskOutputProto()
    {
        return TurboProto.TaskOutput.newBuilder().setTaskId(this.taskId).setOutput(this.output).build();
    }
}
