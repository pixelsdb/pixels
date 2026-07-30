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

import static java.util.Objects.requireNonNull;

/**
 * One immutable capacity decision produced from a stage status snapshot.
 */
public class StageScalingDecision
{
    public enum Direction
    {
        SCALE_OUT,
        STABLE,
        SCALE_IN
    }

    private final int targetWorkerCount;
    private final Direction direction;
    private final String reason;

    public StageScalingDecision(int targetWorkerCount, Direction direction, String reason)
    {
        this.targetWorkerCount = targetWorkerCount;
        this.direction = requireNonNull(direction, "direction is null");
        this.reason = requireNonNull(reason, "reason is null");
    }

    public int getTargetWorkerCount()
    {
        return targetWorkerCount;
    }

    public Direction getDirection()
    {
        return direction;
    }

    public String getReason()
    {
        return reason;
    }
}
