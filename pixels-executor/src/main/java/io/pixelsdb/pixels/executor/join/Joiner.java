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

import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.lambda.domain.PartitionInfo;
import io.pixelsdb.pixels.executor.utils.HashTable;
import io.pixelsdb.pixels.executor.utils.Tuple;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.executor.utils.NullTuple.buildNullTuple;
import static java.util.Objects.requireNonNull;

/**
 * Use one set of row batches from the small table to build a hash map, and join it
 * with the row batches from the big table. In Pixels, we have a default rule that
 * the left table is the small table, and the right table is the big table.
 * <p/>
 * <b>Note 1:</b> this joiner can not be directly used for left outer broadcast
 * distributed join, because it does not ensure that the unmatched tuples from the
 * small table will be returned only once by all the joiners within the join.
 * <p/>
 * <b>Note 2:</b> only equi-joins are supported. Semi-join and anti-join are not supported.
 * Thus, null values are not checked in the join. There is no join result if either or both
 * side(s) is(are) null.
 *
 * @author hank
 * @date 07/05/2022
 */
public class Joiner
{
    private final HashTable smallTable = new HashTable();
    private final JoinType joinType;
    private final TypeDescription smallSchema;
    private final TypeDescription bigSchema;
    private final TypeDescription joinedSchema;
    /**
     * Whether the join-key columns should be included in the {@link #joinedSchema}.
     */
    private final boolean includeKeyCols;
    private final int[] smallKeyColumnIds;
    private final Set<Integer> smallKeyColumnIdSet;
    private final int[] bigKeyColumnIds;
    private final Set<Integer> bigKeyColumnIdSet;
    /**
     * All the tuples from the small table that have been matched during the join.
     */
    private final Set<Tuple> matchedSmallTuples = new HashSet<>();
    private final Tuple smallNullTuple;
    private final Tuple bigNullTuple;

    /**
     * The joiner to join two tables. Currently, only NATURE/INNER/LEFT/RIGHT equi-joins
     * are supported. The first numKeyColumns columns in the two tables are considered as
     * the join key. The small table, a.k.a., the left table, is hash probed in the join.
     * <p/>
     * In the join result, first comes with the columns from the small table, followed by
     * the columns from the big (a.k.a., right) table.
     *
     * @param joinType the join type
     * @param joinedCols the column names in the joined schema, in the same order of the columns
     *                   in small schema and big schema
     * @param includeKeyCols whether joinedCols includes the key columns from both tables.
     * @param smallSchema the schema of the small table
     * @param smallKeyColumnIds the ids of the key columns of the small table
     * @param bigSchema the schema of the big table
     * @param bigKeyColumnIds the ids of the key columns of the big table
     */
    public Joiner(JoinType joinType, String[] joinedCols, boolean includeKeyCols,
                  TypeDescription smallSchema, int[] smallKeyColumnIds,
                  TypeDescription bigSchema, int[] bigKeyColumnIds)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
        requireNonNull(joinedCols, "joinedCols is null");
        checkArgument(joinType != JoinType.UNKNOWN, "joinType is UNKNOWN");
        this.smallSchema = requireNonNull(smallSchema, "smallSchema is null");
        this.bigSchema = requireNonNull(bigSchema, "bigSchema is null");
        checkArgument(smallKeyColumnIds != null && smallKeyColumnIds.length > 0,
                "smallKeyColumnIds is null or empty");
        checkArgument(bigKeyColumnIds != null && bigKeyColumnIds.length > 0,
                "bigKeyColumnIds is null or empty");
        this.smallKeyColumnIds = smallKeyColumnIds;
        this.smallKeyColumnIdSet = new HashSet<>(this.smallKeyColumnIds.length);
        for (int id : this.smallKeyColumnIds)
        {
            this.smallKeyColumnIdSet.add(id);
        }
        this.bigKeyColumnIds = bigKeyColumnIds;
        this.bigKeyColumnIdSet = new HashSet<>(this.bigKeyColumnIds.length);
        for (int id : this.bigKeyColumnIds)
        {
            this.bigKeyColumnIdSet.add(id);
        }
        // build the schema for the join result.
        this.joinedSchema = new TypeDescription(TypeDescription.Category.STRUCT);
        this.includeKeyCols = includeKeyCols;
        List<String> smallColumnNames = smallSchema.getFieldNames();
        List<TypeDescription> smallColumnTypes = smallSchema.getChildren();
        checkArgument(smallColumnTypes != null && smallColumnNames.size() == smallColumnTypes.size(),
                "invalid children of smallSchema");
        List<String> bigColumnNames = bigSchema.getFieldNames();
        List<TypeDescription> bigColumnTypes = bigSchema.getChildren();
        checkArgument(bigColumnTypes != null && bigColumnNames.size() == bigColumnTypes.size(),
                "invalid children of bigSchema");
        int outputColumNum = smallColumnNames.size() + bigColumnNames.size();
        if (this.includeKeyCols)
        {
            outputColumNum -= (joinType == JoinType.NATURAL ? bigKeyColumnIds.length : 0);
        }
        else
        {
            outputColumNum -= (smallKeyColumnIds.length + bigKeyColumnIds.length);
        }
        checkArgument(outputColumNum == joinedCols.length,
                "joinedCols does not contain correct number of elements");
        int joinedColId = 0;
        for (int i = 0; i < smallColumnNames.size(); ++i)
        {
            if ((!this.includeKeyCols) && smallKeyColumnIdSet.contains(i))
            {
                // ignore the join key columns.
                continue;
            }
            this.joinedSchema.addField(joinedCols[joinedColId++], smallColumnTypes.get(i));
        }
        for (int i = 0; i < bigColumnNames.size(); ++i)
        {
            if ((!this.includeKeyCols || joinType == JoinType.NATURAL) && bigKeyColumnIdSet.contains(i))
            {
                // ignore the join key columns.
                // for natural join, the key columns of the right table is ignored.
                continue;
            }
            this.joinedSchema.addField(joinedCols[joinedColId++], bigColumnTypes.get(i));
        }
        // create the null tuples for outer join.
        int numSmallIncludedColumns = this.includeKeyCols ?
                smallColumnNames.size() : smallColumnNames.size() - smallKeyColumnIds.length;
        int numBigIncludedColumns = this.includeKeyCols && this.joinType != JoinType.NATURAL ?
                bigColumnNames.size() : bigColumnNames.size() - bigKeyColumnIds.length;
        this.smallNullTuple = buildNullTuple(numSmallIncludedColumns);
        this.bigNullTuple = buildNullTuple(numBigIncludedColumns);
    }

    /**
     * Populate the hash table for the left (a.k.a., small) table in the join. The
     * hash table will be used for probing in the join.
     * <b>Note</b> this method is thread safe, but it should only be called before
     * {@link Joiner#join(VectorizedRowBatch) join}.
     *
     * @param smallBatch a row batch from the left (a.k.a., small) table
     */
    public synchronized void populateLeftTable(VectorizedRowBatch smallBatch)
    {
        requireNonNull(smallBatch, "smallBatch is null");
        checkArgument(smallBatch.size > 0, "smallBatch is empty");
        Tuple.Builder builder = new Tuple.Builder(smallBatch,
                this.smallKeyColumnIds, this.smallKeyColumnIdSet, this.includeKeyCols);
        while (builder.hasNext())
        {
            Tuple tuple = builder.next();
            this.smallTable.put(tuple);
        }
    }

    /**
     * Perform the join for a row batch from the right (a.k.a., big) table.
     * This method is thread-safe, but should not be called before the small table is populated.
     *
     * @param bigBatch a row batch from the bigger table
     * @return the row batches of the join result, could be empty. <b>Note: </b> the returned
     * list is backed by {@link LinkedList}, thus it is not performant to access it randomly.
     */
    public List<VectorizedRowBatch> join(VectorizedRowBatch bigBatch)
    {
        requireNonNull(bigBatch, "bigBatch is null");
        checkArgument(bigBatch.size > 0, "bigBatch is empty");
        List<VectorizedRowBatch> result = new LinkedList<>();
        VectorizedRowBatch joinedRowBatch = this.joinedSchema.createRowBatch(bigBatch.maxSize);
        Tuple.Builder builder = new Tuple.Builder(bigBatch, this.bigKeyColumnIds, this.bigKeyColumnIdSet,
                this.includeKeyCols && this.joinType != JoinType.NATURAL);
        while (builder.hasNext())
        {
            Tuple big = builder.next();
            Tuple smallHead = this.smallTable.getHead(big);
            if (smallHead == null)
            {
                switch (joinType)
                {
                    case NATURAL:
                    case EQUI_INNER:
                    case EQUI_LEFT:
                        break;
                    case EQUI_RIGHT:
                    case EQUI_FULL:
                        Tuple joined = big.concatLeft(smallNullTuple);
                        if (joinedRowBatch.isFull())
                        {
                            result.add(joinedRowBatch);
                            joinedRowBatch = this.joinedSchema.createRowBatch(bigBatch.maxSize);
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
                    Tuple joined = big.concatLeft(smallHead);
                    if (joinedRowBatch.isFull())
                    {
                        result.add(joinedRowBatch);
                        joinedRowBatch = this.joinedSchema.createRowBatch(bigBatch.maxSize);
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
            this.bigNullTuple.concatLeft(small).writeTo(leftOuterBatch);
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
    public boolean writeLeftOuterAndPartition(PixelsWriter pixelsWriter, int batchSize,
                                              PartitionInfo partitionInfo) throws IOException
    {
        checkArgument(this.joinType == JoinType.EQUI_LEFT || this.joinType == JoinType.EQUI_FULL,
                "getLeftOuter() can only be used for left or full outer join");
        checkArgument(batchSize > 0, "batchSize must be positive");
        requireNonNull(pixelsWriter, "pixelsWriter is null");
        requireNonNull(partitionInfo, "partitionInfo is null");

        Partitioner partitioner = new Partitioner(partitionInfo.getNumParition(),
                batchSize, this.joinedSchema, partitionInfo.getKeyColumnIds());
        List<List<VectorizedRowBatch>> partitioned = new ArrayList<>(partitionInfo.getNumParition());
        for (int i = 0; i < partitionInfo.getNumParition(); ++i)
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
                //pixelsWriter.addRowBatch(leftOuterBatch);
                leftOuterBatch.reset();
            }
            this.bigNullTuple.concatLeft(small).writeTo(leftOuterBatch);
        }
        if (!leftOuterBatch.isEmpty())
        {
            Map<Integer, VectorizedRowBatch> parts = partitioner.partition(leftOuterBatch);
            for (Map.Entry<Integer, VectorizedRowBatch> entry : parts.entrySet())
            {
                partitioned.get(entry.getKey()).add(entry.getValue());
            }
            //pixelsWriter.addRowBatch(leftOuterBatch);
        }
        VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
        for (int hash = 0; hash < tailBatches.length; ++hash)
        {
            if (!tailBatches[hash].isEmpty())
            {
                partitioned.get(hash).add(tailBatches[hash]);
            }
        }
        for (int hash = 0; hash < partitionInfo.getNumParition(); ++hash)
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

    public TypeDescription getJoinedSchema()
    {
        return this.joinedSchema;
    }

    public int getLeftTableSize()
    {
        return this.smallTable.size();
    }
}
