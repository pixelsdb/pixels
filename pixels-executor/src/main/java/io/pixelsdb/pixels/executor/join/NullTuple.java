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

/**
 * The tuple of which all the columns are null.
 * This is mainly used in outer join.
 *
 * @author hank
 * @date 5/10/22
 */
public class NullTuple extends Tuple
{
    private static final ColumnVector[] NON_KEY_COLUMNS = new ColumnVector[0];
    private static final int INVALID_HASH_CODE = Integer.MIN_VALUE;
    private static final int INVALID_ROW_ID = -1;

    private final int numKeyColumns;
    private final int numNonKeyColumns;

    /**
     * For performance considerations, the parameters are not checked.
     * Must ensure that they are valid.
     *
     * @param numKeyColumns the number of key columns
     * @param numNonKeyColumns the number of non-key columns
     * @param joinType the join type
     */
    public NullTuple(int numKeyColumns, int numNonKeyColumns, JoinType joinType)
    {
        super(INVALID_HASH_CODE, INVALID_ROW_ID, NON_KEY_COLUMNS, NON_KEY_COLUMNS, joinType);
        this.numKeyColumns = numKeyColumns;
        this.numNonKeyColumns = numNonKeyColumns;
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
            for (int i = start; i < start + numKeyColumns; ++i)
            {
                rowBatch.cols[i].addNull();
            }
            nextStart += this.numKeyColumns;
        }
        for (int i = nextStart; i < nextStart + this.numNonKeyColumns; ++i)
        {
            rowBatch.cols[i].addNull();
        }
        nextStart += this.numKeyColumns;
        if (next != null)
        {
            next.writeTo(rowBatch, nextStart, this.joinType != JoinType.NATURE);
        }
    }
}
