/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common.metadata.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * Created at: 10/19/21
 * Author: hank
 */
public class Projections
{
    private int numProjections;
    private List<ProjectionPattern> projectionPatterns = new ArrayList<>();

    public int getNumProjections()
    {
        return numProjections;
    }

    public void setNumProjections(int numProjections)
    {
        this.numProjections = numProjections;
    }

    public List<ProjectionPattern> getProjectionPatterns()
    {
        return projectionPatterns;
    }

    public void setProjectionPatterns(List<ProjectionPattern> projectionPatterns)
    {
        this.projectionPatterns = projectionPatterns;
    }

    public void addProjectionPatterns(ProjectionPattern projectionPattern)
    {
        this.projectionPatterns.add(projectionPattern);
    }
}
