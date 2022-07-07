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
package io.pixelsdb.pixels.executor.aggregation;

import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.aggregation.function.Function;
import io.pixelsdb.pixels.executor.utils.Tuple;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 07/07/2022
 */
public class AggrTuple extends Tuple
{
    /**
     * The ids of the columns that are used in aggregation.
     * Currently, we only support single-column aggregation, thus each column id
     * corresponds to an aggregate column and the corresponding aggregate function.
     * TODO: support multi-column aggregation.
     */
    protected final int[] aggrColumnIds;

    private Function[] functions;

    /**
     * For performance considerations, the parameters are not checked.
     * Must ensure that they are valid.
     *
     * @param rowId
     * @param commonFields
     */
    protected AggrTuple(int rowId, CommonFields commonFields, int[] aggrColumnIds)
    {
        super(rowId, commonFields);
        this.aggrColumnIds = aggrColumnIds;
    }

    public void setFunctions(Function[] functions)
    {
        this.functions = functions;
    }

    public void aggregate(AggrTuple other)
    {
        for (int i = 0; i < this.aggrColumnIds.length; ++i)
        {
            this.functions[i].input(other.rowId, other.commonFields.columns[other.aggrColumnIds[i]]);
        }
    }

    @Override
    protected int writeTo(VectorizedRowBatch rowBatch, int start)
    {
        for (int i = 0; i < this.commonFields.keyColumnIds.length; ++i)
        {
            if (this.commonFields.projection[i])
            {
                rowBatch.cols[start++].addElement(this.rowId,
                        this.commonFields.columns[this.commonFields.keyColumnIds[i]]);
            }
        }
        for (Function function : this.functions)
        {
            function.output(rowBatch.cols[start++]);
        }
        return start;
    }

    public static class Builder
    {
        private final int numRows;
        private final CommonFields commonFields;
        private final int[] aggrColumnIds;
        private int rowId = 0;

        /**
         * Create a tuple builder for the row batch.
         *
         * @param rowBatch the row batch
         * @param keyColumnIds the ids of the group-key columns
         * @param projection whether the group-key columns are written into the output
         * @param aggrColumnIds the ids of the columns used for aggregation
         */
        public Builder(VectorizedRowBatch rowBatch, int[] keyColumnIds, boolean[] projection,
                       int[] aggrColumnIds)
        {
            checkArgument(keyColumnIds != null && keyColumnIds.length > 0,
                    "keyColumnIds is null or empty");
            checkArgument(aggrColumnIds != null && aggrColumnIds.length > 0,
                    "aggrColumnIds is null or empty");
            requireNonNull(rowBatch, "rowBatch is null");
            checkArgument(rowBatch.numCols >= keyColumnIds.length,
                    "rowBatch does not have enough columns");
            checkArgument(rowBatch.size > 0, "rowBatch is empty");
            checkArgument(projection != null && keyColumnIds.length == projection.length,
                    "projection is null or has incorrect size");

            ColumnVector[] columns = rowBatch.cols;
            int[] hashCode = new int[rowBatch.size];
            Arrays.fill(hashCode, 0);
            for (int id : keyColumnIds)
            {
                columns[id].accumulateHashCode(hashCode);
            }
            this.numRows = rowBatch.size;
            this.aggrColumnIds = aggrColumnIds;
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
            return new AggrTuple(id, commonFields, aggrColumnIds);
        }
    }
}
