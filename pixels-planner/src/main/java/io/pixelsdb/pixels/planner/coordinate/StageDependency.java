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

/**
 * @author hank
 * @create 2023-09-24
 */
public class StageDependency
{
    private final int currentStageId;
    private final int downStreamStageId;
    private final boolean isWide;

    public StageDependency(int currentStageId, int downStreamStageId, boolean isWide)
    {
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

    public boolean isWide()
    {
        return isWide;
    }
}
