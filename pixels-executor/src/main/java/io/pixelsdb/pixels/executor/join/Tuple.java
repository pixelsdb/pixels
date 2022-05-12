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

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

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
     * The ids of the join-key columns.
     */
    protected final int[] keyColumnIds;

    /**
     * The column vectors in the row batch.
     */
    private final ColumnVector[] columns;

    protected final JoinType joinType;
    /**
     * The next tuple that is joined with this tuple.
     * For equal join, the joined tuples should have the same join-key value.
     */
    protected Tuple next;

    /**
     * For performance considerations, the parameters are not checked.
     * Must ensure that they are valid.
     */
    public Tuple(int hashCode, int rowId, int[] keyColumnIds,
                 ColumnVector[] columns, JoinType joinType)
    {
        this.hashCode = hashCode;
        this.rowId = rowId;
        this.keyColumnIds = keyColumnIds;
        this.columns = columns;
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
            for (int id : this.keyColumnIds)
            {
                if (!this.columns[id].elementEquals(this.rowId, other.rowId, other.columns[id]))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public Tuple join(Tuple next)
    {
        this.next = next;
        return this;
    }

    /**
     * Write the values in this tuple and the joined tuples into the row batch.
     *
     * @param rowBatch the row batch to be written
     */
    public void writeTo(VectorizedRowBatch rowBatch)
    {
        writeTo(rowBatch, 0, true);
    }

    /**
     * Write the values of the non-key columns into the row batch.
     * @param rowBatch the row batch
     * @param start the index of the column in the row batch to start writing
     * @param includeKey whether write the key columns
     */
    protected void writeTo(VectorizedRowBatch rowBatch, int start, boolean includeKey)
    {
        for (int i = 0, j = 0; i < this.columns.length; ++i)
        {
            if (!includeKey && i == this.keyColumnIds[j])
            {
                j++;
                continue;
            }
            rowBatch.cols[start++] = this.columns[i];
        }
        if (next != null)
        {
            next.writeTo(rowBatch, start, this.joinType != JoinType.NATURE);
        }
    }

    public static class Builder
    {
        private final int[] keyColumnIds;
        private final ColumnVector[] columns;
        private final JoinType joinType;
        private final int numRows;
        private final int[] hashCode;
        private int rowId = 0;

        /**
         * Create a tuple builder for the row batch. The first numKeyColumns columns
         * are considered as the columns in the join key.
         *
         * @param rowBatch the row batch
         * @param keyColumnIds the ids of the join-key columns
         * @param joinType the join type
         */
        public Builder(VectorizedRowBatch rowBatch, int[] keyColumnIds, JoinType joinType)
        {
            checkArgument(keyColumnIds != null && keyColumnIds.length > 0,
                    "keyColumnIds is null or empty");
            requireNonNull(rowBatch, "rowBatch is null");
            checkArgument(rowBatch.numCols >= keyColumnIds.length,
                    "rowBatch does not have enough columns");
            checkArgument(rowBatch.size > 0, "rowBatch is empty");
            requireNonNull(joinType, "joinType is null");
            checkArgument(joinType != JoinType.UNKNOWN, "joinType is UNKNOWN");

            this.joinType = joinType;
            this.keyColumnIds = keyColumnIds;
            this.columns = rowBatch.cols;
            this.hashCode = new int[rowBatch.size];
            Arrays.fill(hashCode, 0);
            for (int id : keyColumnIds)
            {
                this.columns[id].accumulateHashCode(this.hashCode);
            }
            this.numRows = rowBatch.size;
        }

        /**
         * @return true if the next tuple is available
         */
        public boolean hasNext()
        {
            return this.numRows > this.rowId;
        }

        /**
         * @return the next tuple in the row batch
         */
        public Tuple next()
        {
            int id = this.rowId++;
            return new Tuple(hashCode[id], id, keyColumnIds, columns, joinType);
        }
    }
}
