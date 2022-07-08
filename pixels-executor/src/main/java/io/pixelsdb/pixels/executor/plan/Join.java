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
 * {@link Join} is the edge between two tables in the join tree.
 * It represents the join type and condition between the two tables.
 *
 * @author hank
 * @date 29/05/2022
 */
public class Join
{
    private final Table leftTable;

    private final Table rightTable;
    /**
     * The alias of the columns that are in the join result and from the left table.
     */
    private final String[] leftColumnAlias;
    /**
     * The alias of the columns that are in the join result and from the right table.
     */
    private final String[] rightColumnAlias;
    /**
     * The indexes of the key columns in the columns that are read from the left table.
     */
    private final int[] leftKeyColumnIds;
    /**
     * The indexes of the key columns in the columns that are read from the right table.
     */
    private final int[] rightKeyColumnIds;
    /**
     * Whether the join result includes the columns from the left table.
     */
    private final boolean[] leftProjection;
    /**
     * Whether the join result includes the columns from the right table.
     */
    private final boolean[] rightProjection;

    private final JoinEndian joinEndian;

    private final JoinType joinType;

    private final JoinAlgorithm joinAlgo;

    public Join(Table leftTable, Table rightTable,
                String[] leftColumnAlias, String[] rightColumnAlias,
                int[] leftKeyColumnIds, int[] rightKeyColumnIds,
                boolean[] leftProjection, boolean[] rightProjection,
                JoinEndian joinEndian, JoinType joinType, JoinAlgorithm joinAlgo)
    {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.leftColumnAlias = leftColumnAlias;
        this.rightColumnAlias = rightColumnAlias;
        this.leftKeyColumnIds = leftKeyColumnIds;
        this.rightKeyColumnIds = rightKeyColumnIds;
        this.leftProjection = leftProjection;
        this.rightProjection = rightProjection;
        this.joinEndian = joinEndian;
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

    public String[] getLeftColumnAlias()
    {
        return leftColumnAlias;
    }

    public String[] getRightColumnAlias()
    {
        return rightColumnAlias;
    }

    public boolean[] getLeftProjection()
    {
        return leftProjection;
    }

    public boolean[] getRightProjection()
    {
        return rightProjection;
    }

    public int[] getLeftKeyColumnIds()
    {
        return leftKeyColumnIds;
    }

    public int[] getRightKeyColumnIds()
    {
        return rightKeyColumnIds;
    }

    public JoinEndian getJoinEndian()
    {
        return joinEndian;
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
