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
package io.pixelsdb.pixels.planner.plan.physical.domain;

import io.pixelsdb.pixels.executor.join.JoinType;

import java.util.Arrays;

/**
 * @author hank
 * @date 03/06/2022
 */
public class ChainJoinInfo extends JoinInfo
{
    /**
     * The alias of the columns in the join result. The alias follows the order of the small
     * table columns first and then the large table columns.
     * <p/>
     * For example, if the small table L scans 3 column A, B, and C, whereas the large table R
     * scans 4 columns D, E, F, and G. The join condition is (L inner join R on L.A=R.B).
     * Then, the joined columns would be A, B, C, D, E, F, and G. And the alias of the joined
     * columns must follow this order, such as A_0, B_1, C_2, D_3, E_4, F_5, and G_6.
     */
    private String[] resultColumns;
    /**
     * The column ids of the key columns in the result columns.
     */
    private int[] keyColumnIds;

    /**
     * Default constructor for Jackson.
     */
    public ChainJoinInfo() { }

    public ChainJoinInfo(JoinType joinType, String[] smallColumnAlias, String[] largeColumnAlias,
                         int[] keyColumnIds, boolean[] smallProjection, boolean[] largeProjection,
                         boolean postPartition, PartitionInfo postPartitionInfo)
    {
        super(joinType, smallColumnAlias, largeColumnAlias, smallProjection, largeProjection,
                postPartition, postPartitionInfo);
        this.keyColumnIds = keyColumnIds;
        this.resultColumns = Arrays.copyOf(smallColumnAlias, smallColumnAlias.length + largeColumnAlias.length);
        System.arraycopy(largeColumnAlias, 0, this.resultColumns, smallColumnAlias.length, largeColumnAlias.length);
    }

    public int[] getKeyColumnIds()
    {
        return keyColumnIds;
    }

    public void setKeyColumnIds(int[] keyColumnIds)
    {
        this.keyColumnIds = keyColumnIds;
    }

    public String[] getResultColumns()
    {
        return resultColumns;
    }
}
