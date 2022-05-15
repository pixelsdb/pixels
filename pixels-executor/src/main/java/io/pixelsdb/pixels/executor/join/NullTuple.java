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
package io.pixelsdb.pixels.executor.join;

import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.util.HashSet;
import java.util.Set;

/**
 * The tuple of which all the columns are null.
 * This is mainly used in outer join.
 *
 * @author hank
 * @date 5/10/22
 */
public class NullTuple extends Tuple
{
    private static final ColumnVector[] EMPTY_COLUMNS = new ColumnVector[0];
    private static final int INVALID_HASH_CODE = Integer.MIN_VALUE;
    private static final int INVALID_ROW_ID = -1;

    private final int numColumns;

    /**
     * Null tuple is mainly used for outer joins. All fields in a null tuple are null.
     *
     * @param keyColumnIds the ids of the key columns
     * @param keyColumnIdSet the id set of the key columns, used for performance consideration
     * @param numColumns the number of columns
     * @param joinType the join type
     */
    protected NullTuple(int[] keyColumnIds, Set<Integer> keyColumnIdSet, int numColumns, JoinType joinType)
    {
        super(INVALID_HASH_CODE, INVALID_ROW_ID, keyColumnIds, keyColumnIdSet, EMPTY_COLUMNS, joinType);
        this.numColumns = numColumns;
    }

    public static NullTuple createNullTuple(int[] keyColumnIds, int numColumns, JoinType joinType)
    {
        Set<Integer> keyColumnIdSet = new HashSet<>(keyColumnIds.length);
        for (int id : keyColumnIds)
        {
            keyColumnIdSet.add(id);
        }
        return new NullTuple(keyColumnIds, keyColumnIdSet, numColumns, joinType);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException("hashCode() is not supported");
    }

    @Override
    public boolean equals(Object obj)
    {
        throw new UnsupportedOperationException("equals() is not supported");
    }

    @Override
    protected int writeTo(VectorizedRowBatch rowBatch, int start)
    {
        if (left != null)
        {
            start = left.writeTo(rowBatch, start);
        }
        // joiner can ensure that all the concatenated (joined) tuples are of the same join type.
        boolean includeKey = left == null || this.joinType != JoinType.NATURAL;
        int numColumnsToWrite = includeKey ? this.numColumns : this.numColumns - this.keyColumnIds.length;
        for (int i = 0; i < numColumnsToWrite; ++i)
        {
            rowBatch.cols[start + i].addNull();
        }
        return start + numColumnsToWrite;
    }
}
