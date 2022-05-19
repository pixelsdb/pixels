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
import java.util.Set;

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
     * The id set of the join-key columns, used for performance consideration.
     */
    protected final Set<Integer> keyColumnIdSet;
    /**
     * The column vectors in the row batch.
     */
    private final ColumnVector[] columns;
    /**
     * The join type that this tuple is used for.
     */
    protected final JoinType joinType;
    /**
     * The left-table tuple that is joined with this tuple.
     * For equal join, the joined tuples should have the same join-key value.
     */
    protected Tuple left;

    /**
     * For performance considerations, the parameters are not checked.
     * Must ensure that they are valid.
     */
    protected Tuple(int hashCode, int rowId, int[] keyColumnIds,
                 Set<Integer> keyColumnIdSet, ColumnVector[] columns, JoinType joinType)
    {
        this.hashCode = hashCode;
        this.rowId = rowId;
        this.keyColumnIds = keyColumnIds;
        this.keyColumnIdSet = keyColumnIdSet;
        this.columns = columns;
        this.joinType = joinType;
        this.left = null;
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
            if (this.keyColumnIds.length != other.keyColumnIds.length)
            {
                return false;
            }
            for (int i = 0; i < this.keyColumnIds.length; ++i)
            {
                // We only support equi-joins, thus null value is not checked.
                if (!this.columns[this.keyColumnIds[i]].elementEquals(
                        this.rowId, other.rowId, other.columns[other.keyColumnIds[i]]))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Concat with the tuple from the left table. The concatenation is directly
     * applied on this tuple.
     *
     * @param left the tuple from the left table
     * @return the concat result, e.i., this tuple
     */
    public Tuple concatLeft(Tuple left)
    {
        this.left = left;
        return this;
    }

    /**
     * Write the values in this tuple and the joined tuples into the row batch.
     *
     * @param rowBatch the row batch to be written
     */
    public void writeTo(VectorizedRowBatch rowBatch)
    {
        writeTo(rowBatch, 0);
        rowBatch.size++;
    }

    /**
     * Write the values of the non-key columns into the row batch.
     * @param rowBatch the row batch
     * @param start the index of the column in the row batch to start writing
     */
    protected int writeTo(VectorizedRowBatch rowBatch, int start)
    {

        if (left != null)
        {
            start = left.writeTo(rowBatch, start);
        }
        // joiner can ensure that all the concatenated (joined) tuples are of the same join type.
        boolean includeKey = left == null || this.joinType != JoinType.NATURAL;
        for (int i = 0; i < this.columns.length; ++i)
        {
            if (!includeKey && this.keyColumnIdSet.contains(i))
            {
                continue;
            }
            rowBatch.cols[start++].addElement(this.rowId, this.columns[i]);
        }
        return start;
    }

    public static class Builder
    {
        private final int[] keyColumnIds;
        private final Set<Integer> keyColumnIdSet;
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
         * @param keyColumnIdSet the id set of the join-key columns, used for performance consideration
         * @param joinType the join type
         */
        public Builder(VectorizedRowBatch rowBatch, int[] keyColumnIds,
                       Set<Integer> keyColumnIdSet, JoinType joinType)
        {
            checkArgument(keyColumnIds != null && keyColumnIds.length > 0,
                    "keyColumnIds is null or empty");
            checkArgument(keyColumnIdSet != null && keyColumnIdSet.size() == keyColumnIds.length,
                    "keyColumnIdSet is null or of an incorrect size");
            requireNonNull(rowBatch, "rowBatch is null");
            checkArgument(rowBatch.numCols >= keyColumnIds.length,
                    "rowBatch does not have enough columns");
            checkArgument(rowBatch.size > 0, "rowBatch is empty");
            requireNonNull(joinType, "joinType is null");
            checkArgument(joinType != JoinType.UNKNOWN, "joinType is UNKNOWN");

            this.joinType = joinType;
            this.keyColumnIds = keyColumnIds;
            this.keyColumnIdSet = keyColumnIdSet;
            this.columns = rowBatch.cols;
            this.hashCode = new int[rowBatch.size];
            Arrays.fill(this.hashCode, 0);
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
            return new Tuple(hashCode[id], id, keyColumnIds, keyColumnIdSet, columns, joinType);
        }
    }
}
