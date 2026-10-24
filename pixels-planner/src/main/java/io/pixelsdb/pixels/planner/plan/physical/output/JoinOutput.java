/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical.output;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The output format for both broadcast and hash partitioned join.
 *
 * @author hank
 * @create 2022-05-07
 */
public class JoinOutput extends NonPartitionOutput
{
    // Present only on coordinator-managed S3QS invocation responses. Keys are
    // stage-local logical task ids, independent of physical invocation order.
    private Map<Integer, JoinOutput> taskOutputs;

    public Map<Integer, JoinOutput> getTaskOutputs()
    {
        return taskOutputs;
    }

    public void setTaskOutputs(Map<Integer, JoinOutput> taskOutputs)
    {
        this.taskOutputs = taskOutputs;
    }

    public void addTaskOutput(int taskId, JoinOutput output)
    {
        if (taskOutputs == null) { taskOutputs = new LinkedHashMap<>(); }
        if (taskOutputs.putIfAbsent(taskId, output) != null)
        {
            throw new IllegalStateException("duplicate logical join task " + taskId);
        }
    }
}
