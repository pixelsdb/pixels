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

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * First-stage policy based only on coordinator-owned logical task pressure.
 */
public class PendingTaskScalingPolicy implements StageScalingPolicy
{
    @Override
    public StageScalingDecision evaluate(StageRuntimeStatus status, StageScalingHistory history,
                                         StageScalingConfig config, long currentTimeMs)
    {
        int desired = status.getDesiredWorkerCount();
        int pending = status.getPendingTaskCount();
        int running = status.getRunningTaskCount();
        int unfinished = pending + running;
        if (unfinished == 0)
        {
            history.resetLowPressureSamples();
            return decision(0, desired, "stage has no unfinished logical tasks");
        }

        int pendingCapacity = divideRoundUp(pending, config.getTargetPendingTasksPerWorker());
        int rawTarget = running + pendingCapacity;
        rawTarget = min(rawTarget, unfinished);
        rawTarget = max(config.getMinWorkers(), min(config.getMaxWorkers(), rawTarget));
        rawTarget = min(rawTarget, unfinished);

        if (rawTarget > desired)
        {
            history.resetLowPressureSamples();
            if (pending == 0)
            {
                return decision(desired, desired, "no pending logical tasks");
            }
            int target = min(rawTarget, desired + config.getMaxScaleOutStep());
            return new StageScalingDecision(target, StageScalingDecision.Direction.SCALE_OUT,
                    "pending logical tasks require additional capacity");
        }
        if (rawTarget == desired)
        {
            history.resetLowPressureSamples();
            return decision(desired, desired, "current capacity matches task pressure");
        }

        int stableSamples = history.recordLowPressureSample();
        boolean cooldownElapsed = history.getLastScaleInTimeMs() == Long.MIN_VALUE ||
                currentTimeMs - history.getLastScaleInTimeMs() >= config.getScaleInCooldownMs();
        if (stableSamples < config.getScaleInStableSamples() || !cooldownElapsed)
        {
            return decision(desired, desired, "waiting for scale-in hysteresis or cooldown");
        }

        int target = max(rawTarget, desired - config.getMaxScaleInStep());
        history.recordScaleIn(currentTimeMs);
        return new StageScalingDecision(target, StageScalingDecision.Direction.SCALE_IN,
                "sustained low logical task pressure");
    }

    private static int divideRoundUp(int value, int divisor)
    {
        return value == 0 ? 0 : (value + divisor - 1) / divisor;
    }

    private static StageScalingDecision decision(int target, int current, String reason)
    {
        StageScalingDecision.Direction direction = target > current ?
                StageScalingDecision.Direction.SCALE_OUT :
                target < current ? StageScalingDecision.Direction.SCALE_IN :
                        StageScalingDecision.Direction.STABLE;
        return new StageScalingDecision(target, direction, reason);
    }
}
