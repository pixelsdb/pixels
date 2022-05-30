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
package io.pixelsdb.pixels.executor.plan;

import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;

/**
 * The join link is the edge between two tables in the join graph.
 * It represents the join type and condition between the two tables,
 * and is used in join order optimization.
 *
 * @author hank
 * @date 29/05/2022
 */
public class JoinLink
{
    private final Table leftTable;
    private final Table rightTable;
    private final JoinType joinType;
    private final JoinAlgorithm joinAlgo;

    public JoinLink(Table leftTable, Table rightTable,
                    JoinType joinType, JoinAlgorithm joinAlgo)
    {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.joinAlgo = joinAlgo;
    }

    public Table getLeftTable()
    {
        return leftTable;
    }

    public Table getRightTable()
    {
        return rightTable;
    }

    public JoinType getJoinType()
    {
        return joinType;
    }

    public JoinAlgorithm getJoinAlgo()
    {
        return joinAlgo;
    }
}
