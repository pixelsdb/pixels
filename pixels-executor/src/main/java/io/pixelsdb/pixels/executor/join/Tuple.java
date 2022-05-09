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
 * The tuple to be used in join.
 *
 * @author hank
 * @date 09/05/2022
 */
public class Tuple
{
    /**
     * The hashCode of the join key of this tuple.
     */
    private final int hashCode;

    /**
     * The index of this tuple in the corresponding row batch.
     */
    private final int rowId;

    /**
     * The join key columns in the row batch.
     */
    private final ColumnVector[] keyColumns;

    /**
     * The other non-key columns in the row batch.
     */
    private final ColumnVector[] nonKeyColumns;

    private final JoinType joinType;
    /**
     * The next tuple that is joined with this tuple.
     * For equal join, the joined tuples should have the same join-key value.
     */
    private Tuple next;

    /**
     * For performance considerations, the parameters are not checked.
     * Must ensure that they are valid.
     */
    public Tuple(int hashCode, int rowId, ColumnVector[] keyColumns,
                 ColumnVector[] nonKeyColumns, JoinType joinType)
    {
        this.hashCode = hashCode;
        this.rowId = rowId;
        this.keyColumns = keyColumns;
        this.nonKeyColumns = nonKeyColumns;
        this.joinType = joinType;
        this.next = null;
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Tuple)
        {
            Tuple other = (Tuple) obj;
            for (int i = 0; i < this.keyColumns.length; ++i)
            {
                // TODO: implement elementEquals
                // if (!this.keyColumns[i].elementEquals(this.rowId, other.keyColumns[i], other.rowId))
                {
                    // return false;
                }
            }
            return true;
        }
        return false;
    }

    public void Join(Tuple next)
    {
        this.next = next;
    }

    /**
     * Write the values in this tuple and the joined tuples into the row batch.
     *
     * @param rowBatch the row batch to be written
     */
    public void writeTo(VectorizedRowBatch rowBatch)
    {
        writeTo(rowBatch, this.keyColumns.length, true);
    }

    /**
     * Write the values of the non-key columns into the row batch.
     * @param rowBatch the row batch
     * @param start the index of the column in the row batch to start writing
     * @param includeKey whether write the key columns
     */
    private void writeTo(VectorizedRowBatch rowBatch, int start, boolean includeKey)
    {
        int nextStart = start;
        if (includeKey)
        {
            for (int i = 0; i < this.keyColumns.length; ++i)
            {
                // TODO: implement addElement
                // rowBatch.cols[i].addElement(this.keyColumns[i], this.rowId);
            }
            nextStart += this.keyColumns.length;
        }
        for (int i = 0; i < this.nonKeyColumns.length; ++i)
        {
            // TODO: implement addElement
            // rowBatch.cols[start + i].addElement(this.nonKeyColumns[i], this.rowId);
        }
        nextStart += this.nonKeyColumns.length;
        if (next != null)
        {
            next.writeTo(rowBatch, nextStart, this.joinType != JoinType.NATURE);
        }
    }
}
