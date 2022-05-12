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

import static com.google.common.base.Preconditions.checkArgument;

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

    private final int[] nonKeyColumnIds;

    /**
     * For performance considerations, the parameters are not checked.
     * Must ensure that they are valid.
     *
     * @param keyColumnIds the ids of the key columns
     * @param numColumns the number of columns
     * @param joinType the join type
     */
    public NullTuple(int[] keyColumnIds, int numColumns, JoinType joinType)
    {
        super(INVALID_HASH_CODE, INVALID_ROW_ID, keyColumnIds, EMPTY_COLUMNS, joinType);
        checkArgument(keyColumnIds != null && keyColumnIds.length > 0,
                "keyColumnIds is null or empty");
        checkArgument(keyColumnIds.length <= numColumns, "numColumns is too small");
        this.nonKeyColumnIds = new int[numColumns - keyColumnIds.length];
        for (int i = 0, j = 0, k = 0; i < numColumns; ++i)
        {
            if (i == keyColumnIds[j])
            {
                j++;
                continue;
            }
            this.nonKeyColumnIds[k++] = i;
        }
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
    protected void writeTo(VectorizedRowBatch rowBatch, int start, boolean includeKey)
    {
        int nextStart = start;
        if (includeKey)
        {
            for (int id : this.keyColumnIds)
            {
                rowBatch.cols[start + id].addNull();
            }
            nextStart += this.keyColumnIds.length;
        }
        for (int id : this.nonKeyColumnIds)
        {
            rowBatch.cols[nextStart + id].addNull();
        }
        nextStart += this.nonKeyColumnIds.length;
        if (next != null)
        {
            next.writeTo(rowBatch, nextStart, this.joinType != JoinType.NATURE);
        }
    }
}
