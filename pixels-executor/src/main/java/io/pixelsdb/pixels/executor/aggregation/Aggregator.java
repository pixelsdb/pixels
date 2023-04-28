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

import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.aggregation.function.Function;
import io.pixelsdb.pixels.executor.aggregation.function.FunctionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 07/07/2022
 */
public class Aggregator
{
    private static final int MIN_NUM_AGGR_TABLES = 41;
    private final List<Map<AggrTuple, AggrTuple>> aggrTables;
    /**
     * The number of aggregate tables. It is an integer multiple of numPartitions if partition is true,
     * or MIN_NUM_AGGR_TABLES if partition is false. It is equal or greater than MIN_NUM_AGGR_TABLES.
     */
    private final int numAggrTables;
    private final int numPartitions;
    private final boolean partition;
    private final int batchSize;
    private final TypeDescription outputSchema;
    private final int[] groupKeyColumnIds;
    private final int[] aggrColumnIds;
    private final List<Integer> groupKeyColumnIdsInResult;
    private final boolean[] groupKeyColumnProjection;
    private final Function[] aggrFunctions;

    /**
     * Create the aggregator to compute the aggregation result.
     *
     * @param batchSize the row batch size of the output
     * @param inputSchema the schema of the input row batches
     * @param groupKeyColumnAlias the column names for the group-key columns in the output
     * @param groupKeyColumnIds the ids of the group-key columns in the input row batch
     * @param groupKeyColumnProjection the projection of the group-key columns in the input row batch
     * @param aggrColumnIds the ids of the aggregate columns in the input row batch
     * @param resultColumnAlias the alias of the result columns in the output row batch
     * @param resultColumnTypes the types of the result columns in the output row batch
     * @param functionTypes the types of the aggregate functions for each aggregation
     * @param partition true if the aggregation result should be partitioned
     * @param numPartitions the number of partitions of partition is true
     */
    public Aggregator(int batchSize, TypeDescription inputSchema,
                      String[] groupKeyColumnAlias, int[] groupKeyColumnIds,
                      boolean[] groupKeyColumnProjection, int[] aggrColumnIds,
                      String[] resultColumnAlias, String[] resultColumnTypes,
                      FunctionType[] functionTypes, boolean partition, int numPartitions)
    {
        requireNonNull(inputSchema, "inputSchema is null");
        requireNonNull(groupKeyColumnAlias, "groupKeyColumnAlias is null");
        requireNonNull(resultColumnAlias, "resultColumnAlias is null");
        requireNonNull(resultColumnTypes, "resultColumnTypes is null");
        requireNonNull(functionTypes, "functionTypes is null");
        this.batchSize = batchSize;
        this.groupKeyColumnIds = requireNonNull(groupKeyColumnIds, "groupKeyColumnIds is null");
        this.aggrColumnIds = requireNonNull(aggrColumnIds, "aggrColumnIds is null");
        this.groupKeyColumnProjection = requireNonNull(groupKeyColumnProjection, "groupKeyColumnProjection is null");
        checkArgument(batchSize > 1, "batchSize must be non-negative");
        checkArgument(groupKeyColumnAlias.length == groupKeyColumnIds.length &&
                groupKeyColumnAlias.length == groupKeyColumnProjection.length,
                "the lengths of column alias, column ids, and projection of group key column are inconsistent");
        checkArgument(resultColumnAlias.length == resultColumnTypes.length,
                "the lengths of column alias and column types of result columns are inconsistent");

        this.outputSchema = new TypeDescription(TypeDescription.Category.STRUCT);
        List<TypeDescription> inputTypes = inputSchema.getChildren();
        requireNonNull(inputTypes, "children types of the inputSchema is null");
        checkArgument(inputTypes.size() >= groupKeyColumnIds.length + aggrColumnIds.length,
                "inputSchema does not contain enough columns");
        this.groupKeyColumnIdsInResult = new ArrayList<>(groupKeyColumnIds.length);
        for (int i = 0; i < groupKeyColumnAlias.length; ++i)
        {
            this.groupKeyColumnIdsInResult.add(i);
            if (groupKeyColumnProjection[i])
            {
                this.outputSchema.addField(groupKeyColumnAlias[i], inputTypes.get(groupKeyColumnIds[i]));
            }
        }
        this.aggrFunctions = new Function[resultColumnAlias.length];
        for (int i = 0; i < resultColumnAlias.length; ++i)
        {
            TypeDescription outputType = TypeDescription.fromString(resultColumnTypes[i]);
            this.outputSchema.addField(resultColumnAlias[i], outputType);
            this.aggrFunctions[i] = FunctionFactory.Instance().createFunction(
                    functionTypes[i], inputTypes.get(aggrColumnIds[i]), outputType);
        }

        this.partition = partition;
        this.numPartitions = numPartitions;
        if (partition)
        {
            checkArgument(numPartitions > 0, "numPartitions must be positive");
            if (numPartitions < MIN_NUM_AGGR_TABLES)
            {
                this.numAggrTables = numPartitions * (int) Math.ceil(MIN_NUM_AGGR_TABLES / (double) numPartitions);
            }
            else
            {
                this.numAggrTables = numPartitions;
            }
        }
        else
        {
            this.numAggrTables = MIN_NUM_AGGR_TABLES;
        }

        this.aggrTables = new ArrayList<>(this.numAggrTables);
        for (int i = 0; i < this.numAggrTables; ++i)
        {
            this.aggrTables.add(new HashMap<>());
        }
    }

    public boolean isPartition()
    {
        return partition;
    }

    public List<Integer> getGroupKeyColumnIdsInResult()
    {
        return groupKeyColumnIdsInResult;
    }

    /**
     * The user of this method must ensure the tuple is not null and the same tuple
     * is only put once. Different tuples with the same value of join key are put
     * into the same bucket.
     *
     * @param inputRowBatch the row batch of the aggregation input
     */
    public void aggregate(VectorizedRowBatch inputRowBatch)
    {
        AggrTuple.Builder builder = new AggrTuple.Builder(
                inputRowBatch, this.groupKeyColumnIds, this.groupKeyColumnProjection, this.aggrColumnIds);
        synchronized (this.aggrTables)
        {
            while (builder.hasNext())
            {
                AggrTuple input = builder.next();
                int index = input.hashCode() % this.numAggrTables;
                Map<AggrTuple, AggrTuple> aggrTable = this.aggrTables.get(index < 0 ? -index : index);
                AggrTuple baseTuple = aggrTable.get(input);
                if (baseTuple != null)
                {
                    baseTuple.aggregate(input);
                } else
                {
                    // Create the functions.
                    Function[] functions = new Function[this.aggrFunctions.length];
                    for (int i = 0; i < this.aggrFunctions.length; ++i)
                    {
                        functions[i] = this.aggrFunctions[i].buildCopy();
                    }
                    input.initFunctions(functions);
                    aggrTable.put(input, input);
                }
            }
        }
    }

    public boolean writeAggrOutput(PixelsWriter pixelsWriter) throws IOException
    {
        VectorizedRowBatch outputRowBatch = this.outputSchema.createRowBatch(this.batchSize);
        if (partition)
        {
            for (int hash = 0; hash < this.numPartitions; ++hash)
            {
                for (int i = hash; i < this.numAggrTables; i += this.numPartitions)
                {
                    Map<AggrTuple, AggrTuple> aggrTable = this.aggrTables.get(i);
                    for (AggrTuple output : aggrTable.values())
                    {
                        if (outputRowBatch.isFull())
                        {
                            pixelsWriter.addRowBatch(outputRowBatch, hash);
                            outputRowBatch.reset();
                        }
                        output.writeTo(outputRowBatch);
                    }
                }
                if (!outputRowBatch.isEmpty())
                {
                    pixelsWriter.addRowBatch(outputRowBatch, hash);
                    outputRowBatch.reset();
                }
            }
        }
        else
        {
            for (Map<AggrTuple, AggrTuple> aggrTable : this.aggrTables)
            {
                for (AggrTuple output : aggrTable.values())
                {
                    if (outputRowBatch.isFull())
                    {
                        pixelsWriter.addRowBatch(outputRowBatch);
                        outputRowBatch.reset();
                    }
                    output.writeTo(outputRowBatch);
                }
            }
            if (!outputRowBatch.isEmpty())
            {
                pixelsWriter.addRowBatch(outputRowBatch);
            }
        }

        outputRowBatch.close();

        return true;
    }

    public TypeDescription getOutputSchema()
    {
        return outputSchema;
    }

    public void clear()
    {
        for (Map<AggrTuple, AggrTuple> aggrTable : this.aggrTables)
        {
            aggrTable.clear();
        }
        this.aggrTables.clear();
    }
}
