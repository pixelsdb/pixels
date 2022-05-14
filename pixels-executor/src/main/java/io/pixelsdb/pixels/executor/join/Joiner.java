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
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Use one set of row batches from the small table to build a hash map, and join it
 * with the row batches from the big table. In Pixels, we have a default rule that
 * the left table is the small table, and the right table is the big table.
 * <p/>
 * <b>Note</b> that this joiner can not be directly used for left outer broadcast
 * distributed join, because it does not ensure that the unmatched tuples from the
 * small table will be returned only once by all the joiners within the join.
 * @author hank
 * @date 07/05/2022
 */
public class Joiner
{
    private final HashMap<Tuple, Tuple> smallTable = new HashMap<>();
    private final JoinType joinType;
    private final TypeDescription smallSchema;
    private final TypeDescription bigSchema;
    private final TypeDescription joinedSchema;
    private final int[] smallKeyColumnIds;
    private final int[] bigKeyColumnIds;
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
     * @param smallPrefix the prefix for the columns from the small table, e.g., "orders."
     * @param smallSchema the schema of the small table
     * @param smallKeyColumnIds the ids of the key columns of the small table
     * @param bigPrefix the prefix for the columns from the big table, e.e., "lineitem."
     * @param bigSchema the schema of the big table
     * @param bigKeyColumnIds the ids of the key columns of the big table
     */
    public Joiner(JoinType joinType,
                  String smallPrefix, TypeDescription smallSchema, int[] smallKeyColumnIds,
                  String bigPrefix, TypeDescription bigSchema, int[] bigKeyColumnIds)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
        checkArgument(joinType != JoinType.UNKNOWN, "joinType is UNKNOWN");
        this.smallSchema = requireNonNull(smallSchema, "smallSchema is null");
        this.bigSchema = requireNonNull(bigSchema, "bigSchema is null");
        checkArgument(smallKeyColumnIds != null && smallKeyColumnIds.length > 0,
                "smallKeyColumnIds is null or empty");
        checkArgument(bigKeyColumnIds != null && bigKeyColumnIds.length > 0,
                "bigKeyColumnIds is null or empty");
        this.smallKeyColumnIds = smallKeyColumnIds;
        this.bigKeyColumnIds = bigKeyColumnIds;
        // build the schema for the join result.
        this.joinedSchema = new TypeDescription(TypeDescription.Category.STRUCT);
        List<String> smallColumnNames = smallSchema.getFieldNames();
        List<TypeDescription> smallColumnTypes = smallSchema.getChildren();
        checkArgument(smallColumnTypes != null && smallColumnNames.size() == smallColumnTypes.size(),
                "invalid children of smallSchema");
        for (int i = 0; i < smallColumnNames.size(); ++i)
        {
            this.joinedSchema.addField(smallPrefix.concat(smallColumnNames.get(i)), smallColumnTypes.get(i));
        }
        List<String> bigColumnNames = bigSchema.getFieldNames();
        List<TypeDescription> bigColumnTypes = bigSchema.getChildren();
        checkArgument(bigColumnTypes != null && bigColumnNames.size() == bigColumnTypes.size(),
                "invalid children of bigSchema");
        // duplicate the join key if not nature join.
        for (int i = 0, j = 0; i < bigColumnNames.size(); ++i)
        {
            if (joinType == JoinType.NATURE && i == bigKeyColumnIds[j])
            {
                j++;
                continue;
            }
            this.joinedSchema.addField(bigPrefix.concat(bigColumnNames.get(i)), bigColumnTypes.get(i));
        }
        // create the null tuples for outer join.
        this.smallNullTuple = new NullTuple(smallKeyColumnIds, smallColumnNames.size(), joinType);
        this.bigNullTuple = new NullTuple(bigKeyColumnIds, bigColumnNames.size(), joinType);
    }

    /**
     * Populate the hash table for the left (a.k.a., small) table in the join. The
     * hash table will be used for probing in the join.
     * <b>Note</b> this method is thread safe, but it should only be called before
     * {@link Joiner#join(VectorizedRowBatch) join}.
     *
     * @param smallBatch a row batch from the small table
     */
    public synchronized void populateLeftTable(VectorizedRowBatch smallBatch)
    {
        requireNonNull(smallBatch, "smallBatch is null");
        checkArgument(smallBatch.size > 0, "smallBatch is empty");
        Tuple.Builder builder = new Tuple.Builder(smallBatch, this.smallKeyColumnIds, this.joinType);
        while (builder.hasNext())
        {
            Tuple tuple = builder.next();
            this.smallTable.put(tuple, tuple);
        }
    }

    /**
     * Perform the join for a row batch from the right (a.k.a., big) table.
     * This method is thread-safe, but should not be called before the small table is populated.
     *
     * @param bigBatch a row batch from the bigger table
     * @return the row batch of the join result, could be empty
     */
    public VectorizedRowBatch join(VectorizedRowBatch bigBatch)
    {
        requireNonNull(bigBatch, "bigBatch is null");
        checkArgument(bigBatch.size > 0, "bigBatch is empty");
        VectorizedRowBatch joinedRowBatch = this.joinedSchema.createRowBatch(bigBatch.size);
        Tuple.Builder builder = new Tuple.Builder(bigBatch, this.bigKeyColumnIds, this.joinType);
        while (builder.hasNext())
        {
            Tuple big = builder.next(), joined = null;
            Tuple small = this.smallTable.get(big);
            if (small == null)
            {
                switch (joinType)
                {
                    case NATURE:
                    case EQUI_INNER:
                    case EQUI_LEFT:
                        break;
                    case EQUI_RIGHT:
                        joined = smallNullTuple.join(big);
                        break;
                    default:
                        throw new UnsupportedOperationException("join type is not supported");
                }
            } else
            {
                if (joinType == JoinType.EQUI_LEFT)
                {
                    this.matchedSmallTuples.add(small);
                }
                joined = small.join(big);
            }
            if (joined != null)
            {
                checkArgument(!joinedRowBatch.isFull(), "joined row batch is too large");
                joined.writeTo(joinedRowBatch);
            }
        }
        return joinedRowBatch;
    }

    /**
     * Get the left outer join results for the tuples from the unmatched small (a.k.a., left) table.
     * This method should be called after {@link Joiner#join(VectorizedRowBatch) join} is done, if
     * the join is left outer join.
     */
    public VectorizedRowBatch getLeftOuter()
    {
        checkArgument(this.joinType == JoinType.EQUI_LEFT,
                "getLeftOuter() is illegal for non-left-outer join");
        List<Tuple> leftOuterTuples = new ArrayList<>();
        for (Tuple small : this.smallTable.keySet())
        {
            if (!this.matchedSmallTuples.contains(small))
            {
                leftOuterTuples.add(small);
            }
        }
        VectorizedRowBatch leftOuterBatch = this.joinedSchema.createRowBatch(leftOuterTuples.size());
        for (Tuple small : leftOuterTuples)
        {
            small.join(this.bigNullTuple).writeTo(leftOuterBatch);
        }
        return leftOuterBatch;
    }
}
