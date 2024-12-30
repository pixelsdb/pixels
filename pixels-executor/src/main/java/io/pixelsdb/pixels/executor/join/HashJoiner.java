/*
 * Copyright 2024 PixelsDB.
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

import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.utils.HashTable;
import io.pixelsdb.pixels.executor.utils.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HashJoiner extends Joiner
{
    private final HashTable smallTable = new HashTable();

    /**
     * The joiner to join two tables. Currently, only NATURE/INNER/LEFT/RIGHT equi-joins
     * are supported. The first numKeyColumns columns in the two tables are considered as
     * the join key. The small table, a.k.a., the left table, is hash probed in the join.
     * <p/>
     * In the join result, first comes with the columns from the small table, followed by
     * the columns from the large (a.k.a., right) table.
     *
     * @param joinType          the join type
     * @param smallSchema       the schema of the small table
     * @param smallColumnAlias  the alias of the columns from the small table. These alias
     *                          are used as the column name in {@link #joinedSchema}
     * @param smallProjection   denotes whether the columns from {@link #smallSchema}
     *                          are included in {@link #joinedSchema}
     * @param smallKeyColumnIds the ids of the key columns of the small table
     * @param largeSchema       the schema of the large table
     * @param largeColumnAlias  the alias of the columns from the large table. These alias
     *                          are used as the column name in {@link #joinedSchema}
     * @param largeProjection   denotes whether the columns from {@link #largeSchema}
     *                          are included in {@link #joinedSchema}
     * @param largeKeyColumnIds the ids of the key columns of the large table
     */
    public HashJoiner(JoinType joinType, TypeDescription smallSchema, String[] smallColumnAlias, boolean[] smallProjection, int[] smallKeyColumnIds, TypeDescription largeSchema, String[] largeColumnAlias, boolean[] largeProjection, int[] largeKeyColumnIds)
    {
        super(joinType, smallSchema, smallColumnAlias, smallProjection, smallKeyColumnIds, largeSchema, largeColumnAlias, largeProjection, largeKeyColumnIds);
    }

    /**
     * Populate the hash table for the left (a.k.a., small) table in the join. The
     * hash table will be used for probing in the join.
     * <b>Note</b> this method is thread safe, but it should only be called before
     * {@link Joiner#join(VectorizedRowBatch) join}.
     *
     * @param smallBatch a row batch from the left (a.k.a., small) table
     */
    @Override
    public void populateLeftTable(VectorizedRowBatch smallBatch)
    {
        requireNonNull(smallBatch, "smallBatch is null");
        checkArgument(smallBatch.size > 0, "smallBatch is empty");
        Tuple.Builder builder = new Tuple.Builder(smallBatch, this.smallKeyColumnIds, this.smallProjection);
        synchronized (this.smallTable)
        {
            while (builder.hasNext())
            {
                Tuple tuple = builder.next();
                this.smallTable.put(tuple);
            }
        }
    }

    /**
     * Perform the join for a row batch from the right (a.k.a., large) table.
     * This method is thread-safe, but should not be called before the small table is populated.
     *
     * @param largeBatch a row batch from the large table
     * @return the row batches of the join result, could be empty. <b>Note: </b> the returned
     * list is backed by {@link LinkedList}, thus it is not performant to access it randomly.
     */
    @Override
    public List<VectorizedRowBatch> join(VectorizedRowBatch largeBatch)
    {
        requireNonNull(largeBatch, "largeBatch is null");
        checkArgument(largeBatch.size > 0, "largeBatch is empty");
        List<VectorizedRowBatch> result = new LinkedList<>();
        VectorizedRowBatch joinedRowBatch = this.joinedSchema.createRowBatch(largeBatch.maxSize);
        Tuple.Builder builder = new Tuple.Builder(largeBatch, this.largeKeyColumnIds, this.largeProjection);
        while (builder.hasNext())
        {
            Tuple large = builder.next();
            Tuple smallHead = this.smallTable.getHead(large);
            if (smallHead == null)
            {
                switch (joinType)
                {
                    case EQUI_INNER:
                    case EQUI_LEFT:
                        break;
                    case EQUI_RIGHT:
                    case EQUI_FULL:
                        Tuple joined = large.concatLeft(smallNullTuple);
                        if (joinedRowBatch.isFull())
                        {
                            result.add(joinedRowBatch);
                            joinedRowBatch = this.joinedSchema.createRowBatch(largeBatch.maxSize);
                        }
                        joined.writeTo(joinedRowBatch);
                        break;
                    default:
                        throw new UnsupportedOperationException("join type is not supported");
                }
            } else
            {
                if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
                {
                    Tuple tmpSmallHead = smallHead;
                    while (tmpSmallHead != null)
                    {
                        this.matchedSmallTuples.add(tmpSmallHead);
                        tmpSmallHead = tmpSmallHead.next;
                    }
                }
                while (smallHead != null)
                {
                    Tuple joined = large.concatLeft(smallHead);
                    if (joinedRowBatch.isFull())
                    {
                        result.add(joinedRowBatch);
                        joinedRowBatch = this.joinedSchema.createRowBatch(largeBatch.maxSize);
                    }
                    joined.writeTo(joinedRowBatch);
                    smallHead = smallHead.next;
                }
            }
        }
        if (!joinedRowBatch.isEmpty())
        {
            result.add(joinedRowBatch);
        }
        return result;
    }

    /**
     * Get the left outer join results for the tuples from the unmatched small (a.k.a., left) table.
     * This method should be called after {@link Joiner#join(VectorizedRowBatch) join} is done, if
     * the join is left outer join.
     */
    @Override
    public boolean writeLeftOuter(PixelsWriter pixelsWriter, int batchSize) throws IOException
    {
        checkArgument(this.joinType == JoinType.EQUI_LEFT || this.joinType == JoinType.EQUI_FULL,
                "getLeftOuter() can only be used for left or full outer join");
        checkArgument(batchSize > 0, "batchSize must be positive");
        requireNonNull(pixelsWriter, "pixelsWriter is null");

        List<Tuple> leftOuterTuples = new ArrayList<>();
        for (Tuple small : this.smallTable)
        {
            if (!this.matchedSmallTuples.contains(small))
            {
                leftOuterTuples.add(small);
            }
        }
        VectorizedRowBatch leftOuterBatch = this.joinedSchema.createRowBatch(batchSize);
        for (Tuple small : leftOuterTuples)
        {
            if (leftOuterBatch.isFull())
            {
                pixelsWriter.addRowBatch(leftOuterBatch);
                leftOuterBatch.reset();
            }
            this.largeNullTuple.concatLeft(small).writeTo(leftOuterBatch);
        }
        if (!leftOuterBatch.isEmpty())
        {
            pixelsWriter.addRowBatch(leftOuterBatch);
        }
        return true;
    }

    /**
     * Get the left outer join results for the tuples from the unmatched small (a.k.a., left) table.
     * This method should be called after {@link Joiner#join(VectorizedRowBatch) join} is done, if
     * the join is left outer join.
     */
    public boolean writeLeftOuterAndPartition(PixelsWriter pixelsWriter, final int batchSize,
                                              final int numPartition, int[] keyColumnIds) throws IOException
    {
        checkArgument(this.joinType == JoinType.EQUI_LEFT || this.joinType == JoinType.EQUI_FULL,
                "getLeftOuter() can only be used for left or full outer join");
        checkArgument(batchSize > 0, "batchSize must be positive");
        requireNonNull(pixelsWriter, "pixelsWriter is null");
        requireNonNull(keyColumnIds, "keyColumnIds is null");

        Partitioner partitioner = new Partitioner(numPartition,
                batchSize, this.joinedSchema, keyColumnIds);
        List<List<VectorizedRowBatch>> partitioned = new ArrayList<>(numPartition);
        for (int i = 0; i < numPartition; ++i)
        {
            partitioned.add(new LinkedList<>());
        }
        List<Tuple> leftOuterTuples = new ArrayList<>();
        for (Tuple small : this.smallTable)
        {
            if (!this.matchedSmallTuples.contains(small))
            {
                leftOuterTuples.add(small);
            }
        }
        VectorizedRowBatch leftOuterBatch = this.joinedSchema.createRowBatch(batchSize);
        for (Tuple small : leftOuterTuples)
        {
            if (leftOuterBatch.isFull())
            {
                Map<Integer, VectorizedRowBatch> parts = partitioner.partition(leftOuterBatch);
                for (Map.Entry<Integer, VectorizedRowBatch> entry : parts.entrySet())
                {
                    partitioned.get(entry.getKey()).add(entry.getValue());
                }
                /* Reset should be fine because the row batches in the partition result should not be
                 * affected by resetting leftOuterBatch. But be careful of this line and double check
                 * it if the left outer result is incorrect.
                 */
                leftOuterBatch.reset();
            }
            this.largeNullTuple.concatLeft(small).writeTo(leftOuterBatch);
        }
        if (!leftOuterBatch.isEmpty())
        {
            Map<Integer, VectorizedRowBatch> parts = partitioner.partition(leftOuterBatch);
            for (Map.Entry<Integer, VectorizedRowBatch> entry : parts.entrySet())
            {
                partitioned.get(entry.getKey()).add(entry.getValue());
            }
        }
        VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
        for (int hash = 0; hash < tailBatches.length; ++hash)
        {
            if (!tailBatches[hash].isEmpty())
            {
                partitioned.get(hash).add(tailBatches[hash]);
            }
        }
        for (int hash = 0; hash < numPartition; ++hash)
        {
            List<VectorizedRowBatch> batches = partitioned.get(hash);
            if (!batches.isEmpty())
            {
                for (VectorizedRowBatch batch : batches)
                {
                    pixelsWriter.addRowBatch(batch, hash);
                }
            }
        }
        partitioned.clear();
        return true;
    }


    @Override
    public int getSmallTableSize()
    {
        return this.smallTable.size();
    }

}
