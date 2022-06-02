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

import com.google.common.collect.ImmutableSet;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

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
    private static final int[] EMPTY_KEY_COLUMN_IDS = new int[0];
    private static final Set<Integer> EMPTY_KEY_COLUMN_ID_SET = ImmutableSet.of();
    private static final int INVALID_HASH_CODE = Integer.MIN_VALUE;
    private static final int INVALID_ROW_ID = -1;

    private final int numColumns;

    /**
     * Null tuple is mainly used for outer joins. All fields in a null tuple are null.
     *
     * @param numColumns the number of columns to be written into the output
     */
    protected NullTuple(int numColumns)
    {
        super(INVALID_HASH_CODE, INVALID_ROW_ID, EMPTY_KEY_COLUMN_IDS, EMPTY_KEY_COLUMN_ID_SET,
                EMPTY_COLUMNS, false);
        this.numColumns = numColumns;
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
        for (int i = 0; i < this.numColumns; ++i)
        {
            rowBatch.cols[start + i].addNull();
        }
        return start + this.numColumns;
    }
}
