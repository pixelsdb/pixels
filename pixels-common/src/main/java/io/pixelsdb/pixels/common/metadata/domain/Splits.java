/*
 * Copyright 2019 PixelsDB.
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

public class Splits
{
    private int numRowGroupInBlock;
    private List<OriginSplitPattern> splitPatterns = new ArrayList<>();

    public int getNumRowGroupInBlock()
    {
        return numRowGroupInBlock;
    }

    public void setNumRowGroupInBlock(int numRowGroupInBlock)
    {
        this.numRowGroupInBlock = numRowGroupInBlock;
    }

    public List<OriginSplitPattern> getSplitPatterns()
    {
        return splitPatterns;
    }

    public void setSplitPatterns(List<OriginSplitPattern> splitPatterns)
    {
        this.splitPatterns = splitPatterns;
    }

    public void addSplitPatterns(OriginSplitPattern splitPattern)
    {
        this.splitPatterns.add(splitPattern);
    }
}
