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

import java.util.List;

/**
 * @author hank
 * @create 2023-10-04
 */
public class TaskBatch
{
    private final boolean endOfTasks;
    private final List<TaskInfo> tasks;

    public TaskBatch(boolean endOfTasks, List<TaskInfo> tasks)
    {
        this.endOfTasks = endOfTasks;
        this.tasks = tasks;
    }

    public boolean isEndOfTasks()
    {
        return endOfTasks;
    }

    public List<TaskInfo> getTasks()
    {
        return tasks;
    }
}
