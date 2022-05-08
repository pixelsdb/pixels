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

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Partition a set of row batches into N sets of row batches.
 * Each result row batch set corresponds to a hash partition.
 *
 * @author hank
 * @date 07/05/2022
 */
public class Partitioner
{
    private final int numPartition;
    private final int batchSize;
    private final TypeDescription schema;
    private final List<Integer> partColumnIds;
    private final VectorizedRowBatch[] rowBatches;

    /**
     * Create a partitioner to partition the input row batches into a number of
     * partitions, using the hash function (abs(key.hashCode())%numPartition).
     * <p/>
     * For combined partition key, the hash code of the key is computed as:
     * <blockquote>
     * col[0].hashCode()*31^(n-1) + col[1].hashCode()*31^(n-2) + ... + col[n-1].hashCode()
     * </blockquote>
     * Where col[i] is the value of the ith column in the partition key.
     *
     * @param numPartition the number of partitions
     * @param batchSize the number of rows in each output row batches
     * @param schema the schema of the input and output row batches
     */
    public Partitioner(int numPartition, int batchSize, TypeDescription schema, List<Integer> partColumnIds)
    {
        checkArgument(numPartition > 0, "partitionNum must be positive");
        checkArgument(batchSize > 0, "batchSize must be positive");
        requireNonNull(schema, "schema is null");
        requireNonNull(schema.getChildren(), "schema is empty");
        checkArgument(partColumnIds != null && !partColumnIds.isEmpty(),
                "partColumnIds must be not null and not empty");
        this.numPartition = numPartition;
        this.batchSize = batchSize;
        this.schema = schema;
        this.partColumnIds = partColumnIds;
        this.rowBatches = new VectorizedRowBatch[numPartition];
        for (int i = 0; i < numPartition; ++i)
        {
            this.rowBatches[i] = schema.createRowBatch(batchSize);
        }
    }

    /**
     * Partition the rows in the input row batch.
     * @param input the input row batch
     * @return the output row batches that are full.
     */
    Map<Integer, VectorizedRowBatch> partition(VectorizedRowBatch input)
    {
        requireNonNull(input, "input is null");
        // this.schema.getChildren() has been checked not null.
        checkArgument(input.numCols == this.schema.getChildren().size(),
                "input.numCols does not match the number of fields in the schema");
        int[] hashCode = getHashCode(input);
        Map<Integer, VectorizedRowBatch> output = new HashMap<>();

        for (int i = 0; i < hashCode.length; ++i)
        {
            int hashKey = Math.abs(hashCode[i]) % this.numPartition;
        }
        return null;
    }

    /**
     * Get the hash code of the partition key of the rows in the input row batch.
     * @param input the input row batch
     * @return the hash code
     */
    private int[] getHashCode(VectorizedRowBatch input)
    {
        int[] hashCode = new int[input.size];
        Arrays.fill(hashCode, 0);
        ColumnVector[] columns = input.cols;
        for (int columnId : this.partColumnIds)
        {
            columns[columnId].accumulateHashCode(hashCode);
        }
        return hashCode;
    }

    public int getNumPartition()
    {
        return numPartition;
    }
}
