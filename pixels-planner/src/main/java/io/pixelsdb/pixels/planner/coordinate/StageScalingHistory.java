/*
 * Copyright 2026 PixelsDB.
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

/**
 * Minimal history needed for scale-in hysteresis and cooldown.
 */
public class StageScalingHistory
{
    private int consecutiveLowPressureSamples;
    private long lastScaleInTimeMs = Long.MIN_VALUE;

    public int recordLowPressureSample()
    {
        return ++consecutiveLowPressureSamples;
    }

    public void resetLowPressureSamples()
    {
        consecutiveLowPressureSamples = 0;
    }

    public int getConsecutiveLowPressureSamples()
    {
        return consecutiveLowPressureSamples;
    }

    public long getLastScaleInTimeMs()
    {
        return lastScaleInTimeMs;
    }

    public void recordScaleIn(long timeMs)
    {
        lastScaleInTimeMs = timeMs;
        consecutiveLowPressureSamples = 0;
    }
}
