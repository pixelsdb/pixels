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
package io.pixelsdb.pixels.executor.utils;

import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The tuple to be used in join and extended by the AggrTuple used by aggregation.
 *
 * @author hank
 * @date 09/05/2022
 */
public class Tuple implements Comparable<Tuple>
{
    protected static class CommonFields
    {
        /**
         * The hashCode of the join key of the tuples.
         */
        public final int[] hashCode;
        /**
         * The ids of the key columns.
         */
        public final int[] keyColumnIds;
        /**
         * The column vectors in the row batch.
         */
        public final ColumnVector[] columns;
        /**
         * Whether the vectors of the columns are written into the output.
         */
        public final boolean[] projection;

        public CommonFields(int[] hashCode, int[] keyColumnIds,
                            ColumnVector[] columns, boolean[] projection)
        {
            this.hashCode = hashCode;
            this.keyColumnIds = keyColumnIds;
            this.columns = columns;
            this.projection = projection;
        }
    }

    /**
     * The index of this tuple in the corresponding row batch.
     */
    public final int rowId;
    /**
     * The common fields that are shared by all the tuples from the same row batch.
     */
    public final CommonFields commonFields;
    /**
     * The left-table tuple that is joined with this tuple.
     * For equal join, the joined tuples should have the same join-key value.
     */
    protected Tuple left = null;
    /**
     * The next tuple in the chain. Tuple chain is mainly used to build the hash
     * table of the left table in joins. For performance consideration, we let it
     * be protected instead of private, but be careful when you change it.
     */
    public Tuple next = null;

    /**
     * For performance considerations, the parameters are not checked.
     * Must ensure that they are valid.
     */
    protected Tuple(int rowId, CommonFields commonFields)
    {
        this.rowId = rowId;
        this.commonFields = commonFields;
    }

    @Override
    public int hashCode()
    {
        return commonFields.hashCode[rowId];
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null || getClass() != obj.getClass())
        {
            return false;
        }
        Tuple other = (Tuple) obj;
        if (this.commonFields.keyColumnIds.length != other.commonFields.keyColumnIds.length)
        {
            return false;
        }
        for (int i = 0; i < this.commonFields.keyColumnIds.length; ++i)
        {
            // We only support equi-joins, thus null value is considered not equal to anything.
            if (!this.commonFields.columns[this.commonFields.keyColumnIds[i]].elementEquals(
                    this.rowId, other.rowId, other.commonFields.columns[other.commonFields.keyColumnIds[i]]))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int compareTo(Tuple other)
    {
        if (this == other)
        {
            return 0;
        }
        if (this.commonFields.keyColumnIds.length != other.commonFields.keyColumnIds.length)
        {
            return Integer.compare(this.commonFields.keyColumnIds.length, other.commonFields.keyColumnIds.length);
        }
        for (int i = 0; i < this.commonFields.keyColumnIds.length; ++i)
        {
            int c = this.commonFields.columns[this.commonFields.keyColumnIds[i]].compareElement(
                    this.rowId, other.rowId, other.commonFields.columns[other.commonFields.keyColumnIds[i]]);
            if (c != 0)
            {
                return c;
            }
        }
        return 0;
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
        if (this.left != null)
        {
            start = this.left.writeTo(rowBatch, start);
        }
        for (int i = 0; i < this.commonFields.columns.length; ++i)
        {
            if (this.commonFields.projection[i])
            {
                rowBatch.cols[start++].addElement(this.rowId, this.commonFields.columns[i]);
            }
        }
        return start;
    }

    public static class Builder
    {
        private final int numRows;
        private final CommonFields commonFields;
        private int rowId = 0;

        /**
         * Create a tuple builder for the row batch.
         *
         * @param rowBatch the row batch
         * @param keyColumnIds the ids of the join-key columns
         * @param projection whether the columns are written into the output
         */
        public Builder(VectorizedRowBatch rowBatch, int[] keyColumnIds, boolean[] projection)
        {
            checkArgument(keyColumnIds != null && keyColumnIds.length > 0,
                    "keyColumnIds is null or empty");
            requireNonNull(rowBatch, "rowBatch is null");
            checkArgument(rowBatch.numCols >= keyColumnIds.length,
                    "rowBatch does not have enough columns");
            checkArgument(rowBatch.size > 0, "rowBatch is empty");
            checkArgument(projection != null && rowBatch.numCols == projection.length,
                    "projection is null or has incorrect size");

            ColumnVector[] columns = rowBatch.cols;
            int[] hashCode = new int[rowBatch.size];
            Arrays.fill(hashCode, 0);
            for (int id : keyColumnIds)
            {
                columns[id].accumulateHashCode(hashCode);
            }
            this.numRows = rowBatch.size;

            this.commonFields = new CommonFields(hashCode, keyColumnIds, columns, projection);
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
            return new Tuple(id, commonFields);
        }
    }
}
