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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The dependency of a query execution stage.
 * @author hank
 * @create 2023-09-24
 */
public class StageDependency
{
    private final int currentStageId;
    /**
     * {@link #downStreamStageId} can be negative if there is no valid downstream stage
     */
    private final int downStreamStageId;
    private final boolean isWide;

    /**
     * Create a dependency between the current stage and the downstream (parent) stage.
     * @param currentStageId the id of the current stage
     * @param downStreamStageId the id of the downstream stage
     * @param isWide whether this dependency is wide or not
     */
    public StageDependency(int currentStageId, int downStreamStageId, boolean isWide)
    {
        checkArgument(currentStageId >= 0, "currentStageId must be non-negative");
        this.currentStageId = currentStageId;
        this.downStreamStageId = downStreamStageId;
        this.isWide = isWide;
    }

    public int getCurrentStageId()
    {
        return currentStageId;
    }

    public int getDownStreamStageId()
    {
        return downStreamStageId;
    }

    public boolean hasDownstreamStage()
    {
        return this.downStreamStageId >= 0;
    }

    /**
     * A wide dependency is an m:n dependency between the current and the downstream stages,
     * whereas a narrow dependency is an 1:1 dependency between the current and the downstream stages.
     * @return true if this is a wide dependency, and vice versa
     */
    public boolean isWide()
    {
        return isWide;
    }
}
