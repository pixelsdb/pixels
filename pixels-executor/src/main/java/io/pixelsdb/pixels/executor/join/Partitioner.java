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

import com.google.common.primitives.Ints;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.util.*;

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
    private final int[] keyColumnIds;
    private final VectorizedRowBatch[] rowBatches;

    /**
     * Create a partitioner to partition the input row batches into a number of
     * partitions, using the hash function (abs(key.hashCode())%numPartition).
     * <p/>
     * For combined partition key, the key column must be the first numKeyColumns columns
     * int the input row batches. And the hash code of the key is computed as:
     * <blockquote>
     * col[0].hashCode()*31^(n-1) + col[1].hashCode()*31^(n-2) + ... + col[n-1].hashCode()
     * </blockquote>
     * Where col[i] is the value of the ith column in the partition key.
     *
     * @param numPartition the number of partitions
     * @param batchSize the number of rows in each output row batches
     * @param schema the schema of the input and output row batches
     * @param keyColumnIds the ids of the key columns
     */
    public Partitioner(int numPartition, int batchSize, TypeDescription schema, int[] keyColumnIds)
    {
        checkArgument(numPartition > 0, "partitionNum must be positive");
        checkArgument(batchSize > 0, "batchSize must be positive");
        requireNonNull(schema, "schema is null");
        requireNonNull(schema.getChildren(), "schema is empty");
        checkArgument(keyColumnIds != null && keyColumnIds.length > 0,
                "keyColumnIds is null or empty");
        this.numPartition = numPartition;
        this.batchSize = batchSize;
        this.schema = schema;
        this.keyColumnIds = keyColumnIds;
        this.rowBatches = new VectorizedRowBatch[numPartition];
        for (int i = 0; i < numPartition; ++i)
        {
            this.rowBatches[i] = schema.createRowBatch(batchSize);
        }
    }

    /**
     * Partition the rows in the input row batch. This method is not thread-safe.
     *
     * @param input the input row batch
     * @return the output row batches that are full.
     */
    public Map<Integer, VectorizedRowBatch> partition(VectorizedRowBatch input)
    {
        requireNonNull(input, "input is null");
        checkArgument(input.size <= batchSize, "input is oversize");
        // this.schema.getChildren() has been checked not null.
        checkArgument(input.numCols == this.schema.getChildren().size(),
                "input.numCols does not match the number of fields in the schema");
        int[] hashCode = getHashCode(input);
        List<List<Integer>> selectedArrays = new ArrayList<>(numPartition);
        for (int i = 0; i < numPartition; ++i)
        {
            selectedArrays.add(new ArrayList<>());
        }
        for (int i = 0; i < hashCode.length; ++i)
        {
            int hashKey = Math.abs(hashCode[i]) % this.numPartition;
            // add the row id to the selected array of the partition.
            selectedArrays.get(hashKey).add(i);
        }

        Map<Integer, VectorizedRowBatch> output = new HashMap<>();
        for (int i = 0; i < numPartition; ++i)
        {
            int[] selected = Ints.toArray(selectedArrays.get(i));
            int freeSlots = rowBatches[i].freeSlots();
            if (freeSlots == 0)
            {
                output.put(i, rowBatches[i]);
                rowBatches[i] = schema.createRowBatch(batchSize);
                rowBatches[i].addSelected(selected, 0, selected.length, input);
            }
            else if (freeSlots <= selected.length)
            {
                rowBatches[i].addSelected(selected, 0, freeSlots, input);
                output.put(i, rowBatches[i]);
                rowBatches[i] = schema.createRowBatch(batchSize);
                if (freeSlots < selected.length)
                {
                    rowBatches[i].addSelected(selected, freeSlots,
                            selected.length - freeSlots, input);
                }
            }
            else
            {
                rowBatches[i].addSelected(selected, 0, selected.length, input);
            }
        }
        return output;
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
        for (int columnId : keyColumnIds)
        {
            input.cols[columnId].accumulateHashCode(hashCode);
        }
        return hashCode;
    }

    public int getNumPartition()
    {
        return numPartition;
    }

    public VectorizedRowBatch[] getRowBatches()
    {
        return rowBatches;
    }
}
