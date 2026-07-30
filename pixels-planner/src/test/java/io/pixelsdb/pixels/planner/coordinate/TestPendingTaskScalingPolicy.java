/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public License
 * along with Pixels.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.coordinate;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPendingTaskScalingPolicy
{
    private final PendingTaskScalingPolicy policy = new PendingTaskScalingPolicy();
    private final StageScalingConfig config =
            new StageScalingConfig(true, 1000L, 1, 1, 8, 1, 4, 1, 3, 5000L);

    @Test
    public void scalesOutFromLogicalTaskPressureWithinStepLimit()
    {
        StageScalingDecision decision = policy.evaluate(
                status(1, 1, 8, 1, false), new StageScalingHistory(), config, 1000L);

        assertEquals(StageScalingDecision.Direction.SCALE_OUT, decision.getDirection());
        assertEquals(5, decision.getTargetWorkerCount());
    }

    @Test
    public void neverCreatesMoreWorkersThanUnfinishedLogicalTasks()
    {
        StageScalingConfig immediateScaleIn =
                new StageScalingConfig(true, 1000L, 1, 1, 8, 1, 4, 8, 1, 0L);
        StageScalingDecision decision = policy.evaluate(
                status(7, 7, 1, 1, false), new StageScalingHistory(), immediateScaleIn, 1000L);

        assertEquals(StageScalingDecision.Direction.SCALE_IN, decision.getDirection());
        assertEquals(2, decision.getTargetWorkerCount());
    }

    @Test
    public void scaleInRequiresStableSamplesAndHonorsCooldown()
    {
        StageScalingHistory history = new StageScalingHistory();
        StageRuntimeStatus lowPressure = status(5, 5, 0, 2, false);

        assertEquals(StageScalingDecision.Direction.STABLE,
                policy.evaluate(lowPressure, history, config, 1000L).getDirection());
        assertEquals(StageScalingDecision.Direction.STABLE,
                policy.evaluate(lowPressure, history, config, 2000L).getDirection());
        StageScalingDecision firstScaleIn = policy.evaluate(lowPressure, history, config, 3000L);
        assertEquals(StageScalingDecision.Direction.SCALE_IN, firstScaleIn.getDirection());
        assertEquals(4, firstScaleIn.getTargetWorkerCount());

        assertEquals(StageScalingDecision.Direction.STABLE,
                policy.evaluate(status(4, 4, 0, 2, false), history, config, 4000L).getDirection());
        assertEquals(StageScalingDecision.Direction.STABLE,
                policy.evaluate(status(4, 4, 0, 2, false), history, config, 5000L).getDirection());
        assertEquals(StageScalingDecision.Direction.STABLE,
                policy.evaluate(status(4, 4, 0, 2, false), history, config, 6000L).getDirection());
        assertEquals(StageScalingDecision.Direction.SCALE_IN,
                policy.evaluate(status(4, 4, 0, 2, false), history, config, 8000L).getDirection());
    }

    @Test
    public void terminalStageTargetsZeroWorkers()
    {
        StageScalingDecision decision = policy.evaluate(
                status(3, 3, 0, 0, true), new StageScalingHistory(), config, 1000L);

        assertEquals(StageScalingDecision.Direction.SCALE_IN, decision.getDirection());
        assertEquals(0, decision.getTargetWorkerCount());
    }

    private static StageRuntimeStatus status(int desiredWorkers, int activeAttempts,
                                             int pendingTasks, int runningTasks,
                                             boolean logicalTasksTerminal)
    {
        int completedTasks = logicalTasksTerminal ? 1 : 0;
        int totalTasks = pendingTasks + runningTasks + completedTasks;
        return new StageRuntimeStatus(desiredWorkers, activeAttempts, 0,
                activeAttempts, activeAttempts, 0, pendingTasks, runningTasks,
                completedTasks, 0, totalTasks, logicalTasksTerminal, 0L);
    }
}
