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
package io.pixelsdb.pixels.executor.aggregation.function;

import io.pixelsdb.pixels.core.vector.ColumnVector;

/**
 * The interface for aggregate functions, such as sum, min, max.
 *
 * @author hank
 * @date 07/07/2022
 */
public interface Function
{
    /**
     * Input an element into this function. This is used by the single-column aggregations.
     *
     * @param rowId the index of the element in the input vector
     * @param inputVector the input vector
     */
    void input(int rowId, ColumnVector inputVector);

    /**
     * Input a set of elements into this function. This is used by multi-column aggregations.
     * Each element is an argument of the expression of the aggregation.
     *
     * @param rowId the index of the elements in the input vectors
     * @param inputVectors the input vectors
     */
    void input(int rowId, ColumnVector... inputVectors);

    /**
     * Output the aggregation result into the output vector. This is used by the aggregation
     * that generates a single-value result, e.g., sum(), min(), max().
     *
     * @param outputVector the output vector
     */
    void output(ColumnVector outputVector);

    /**
     * Output the aggregation result into the output vectors. This is used by the aggregation
     * that generates a multi-value result, e.g., for avg(), we have to write two values,
     * sum and count, into the output.
     *
     * @param outputVectors the output vectors
     */
    void output(ColumnVector... outputVectors);

    Function buildCopy();
}
