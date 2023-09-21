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

import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.task.TaskQueue;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;

import java.util.Map;

/**
 * @author hank
 * @create 2023-09-22
 */
public class StageCoordinator
{
    private TaskQueue<Task<? extends Input>> taskQueue;
    private Map<Long, Worker> workers;

    public StageCoordinator()
    {
        taskQueue = new TaskQueue<>();
        taskQueue.offerPending(new Task<>("", new PartitionInput()));
    }
}
