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
import io.pixelsdb.pixels.executor.utils.Tuple;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SortedJoiner extends Joiner
{
    public final List<Tuple> sortedSmallTable = new ArrayList<>();
    public final List<List<Tuple>> tempTable = new ArrayList<>();

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
    public SortedJoiner(JoinType joinType, TypeDescription smallSchema, String[] smallColumnAlias, boolean[] smallProjection, int[] smallKeyColumnIds, TypeDescription largeSchema, String[] largeColumnAlias, boolean[] largeProjection, int[] largeKeyColumnIds)
    {
        super(joinType, smallSchema, smallColumnAlias, smallProjection, smallKeyColumnIds, largeSchema, largeColumnAlias, largeProjection, largeKeyColumnIds);
    }

    public void populateLeftTable(VectorizedRowBatch smallBatch, int partIndex)
    {
        requireNonNull(smallBatch, "smallBatch is null");
        checkArgument(smallBatch.size > 0, "smallBatch is empty");
        if (tempTable.size() <= partIndex)
        {
            tempTable.add(new ArrayList<>());
        }
        List<Tuple> table = tempTable.get(partIndex);

        Tuple.Builder builder = new Tuple.Builder(smallBatch, this.smallKeyColumnIds, this.smallProjection);
        synchronized (table)
        {
            while (builder.hasNext())
            {
                Tuple tuple = builder.next();
                table.add(tuple);
            }
        }

    }

    public void mergeLeftTable()
    {
        class Pair
        {
            Tuple tuple;
            int listIndex;
            int tupleIndex;

            public Pair(Tuple tuple, int listIndex, int tupleIndex)
            {
                this.tuple = tuple;
                this.listIndex = listIndex;
                this.tupleIndex = tupleIndex;
            }
        }

        PriorityQueue<Pair> pq = new PriorityQueue<>(Comparator.comparing(p -> p.tuple));
        int index = 0;
        for (List<Tuple> table : tempTable)
        {
            if (!table.isEmpty())
            {
                pq.add(new Pair(table.get(0), index, 0));
            }
            index++;
        }

        while (!pq.isEmpty())
        {
            Pair pair = pq.poll();
            Tuple minTuple = pair.tuple;
            sortedSmallTable.add(minTuple);
            int listIndex = pair.listIndex;
            int nextTupleIndex = pair.tupleIndex + 1;
            if (nextTupleIndex < tempTable.get(listIndex).size())
            {
                pq.add(new Pair(tempTable.get(listIndex).get(nextTupleIndex), listIndex, nextTupleIndex));
            }
        }
    }

    @Override
    public List<VectorizedRowBatch> join(VectorizedRowBatch largeBatch)
    {
        requireNonNull(largeBatch, "largeBatch is null");
        checkArgument(largeBatch.size > 0, "largeBatch is empty");
        VectorizedRowBatch joinedRowBatch = this.joinedSchema.createRowBatch(largeBatch.maxSize);
        Tuple.Builder builder = new Tuple.Builder(largeBatch, this.largeKeyColumnIds, this.largeProjection);
        List<Tuple> sortedPartLargeTable = new ArrayList<>();

        while (builder.hasNext())
        {
            Tuple tuple = builder.next();
            sortedPartLargeTable.add(tuple);
        }

        List<VectorizedRowBatch> result = new LinkedList<>();
        int i = 0, j = 0;
        while (i < sortedSmallTable.size() || j < sortedPartLargeTable.size())
        {

            Tuple left = (i < sortedSmallTable.size()) ? sortedSmallTable.get(i) : null;
            Tuple right = (j < sortedPartLargeTable.size()) ? sortedPartLargeTable.get(j) : null;

            if (left != null && right != null)
            {
                int comparison = left.compareTo(right);

                if (comparison == 0)
                {
                    if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
                    {
                        this.matchedSmallTuples.add(sortedSmallTable.get(i));
                    }
                    int firstLeft = i, lastLeft = i, firstRight = j, lastRight = j;
                    int ti = i + 1, tj = j + 1;
                    while (ti < sortedSmallTable.size() && (sortedSmallTable.get(ti).compareTo(sortedSmallTable.get(ti - 1)) == 0))
                    {
                        ti++;
                    }
                    lastLeft = ti - 1;

                    while (tj < sortedPartLargeTable.size() && (sortedPartLargeTable.get(tj).compareTo(sortedPartLargeTable.get(tj - 1)) == 0))
                    {
                        tj++;
                    }
                    lastRight = tj - 1;

                    for (int p = firstLeft; p <= lastLeft; p++)
                    {
                        for (int q = firstRight; q <= lastRight; q++)
                        {
                            Tuple joined = sortedPartLargeTable.get(q).concatLeft(sortedSmallTable.get(p));
                            if (joinedRowBatch.isFull())
                            {
                                result.add(joinedRowBatch);
                                joinedRowBatch = this.joinedSchema.createRowBatch(largeBatch.maxSize);
                            }
                            joined.writeTo(joinedRowBatch);
                        }
                    }
                    i = lastLeft + 1;
                    j = lastRight + 1;
                } else if (comparison < 0)
                {
                    i++;
                } else
                {
                    if (joinType == JoinType.EQUI_RIGHT || joinType == JoinType.EQUI_FULL)
                    {
                        Tuple joined = right.concatLeft(smallNullTuple);
                        if (joinedRowBatch.isFull())
                        {
                            result.add(joinedRowBatch);
                            joinedRowBatch = this.joinedSchema.createRowBatch(largeBatch.maxSize);
                        }
                        joined.writeTo(joinedRowBatch);
                    }
                    j++;
                }
            } else
            {
                if (left != null)
                {
                    break;
                } else if (right != null && (joinType == JoinType.EQUI_RIGHT || joinType == JoinType.EQUI_FULL))
                {
                    Tuple joined = right.concatLeft(smallNullTuple);
                    if (joinedRowBatch.isFull())
                    {
                        result.add(joinedRowBatch);
                        joinedRowBatch = this.joinedSchema.createRowBatch(largeBatch.maxSize);
                    }
                    joined.writeTo(joinedRowBatch);
                    j++;
                } else
                {
                    break;
                }

            }
        }

        if (!joinedRowBatch.isEmpty())
        {
            result.add(joinedRowBatch);
        }

        return result;
    }

    @Override
    public void populateLeftTable(VectorizedRowBatch smallBatch)
    {
        populateLeftTable(smallBatch, 0);
    }

    @Override
    public boolean writeLeftOuter(PixelsWriter pixelsWriter, int batchSize) throws IOException
    {
        checkArgument(this.joinType == JoinType.EQUI_LEFT || this.joinType == JoinType.EQUI_FULL,
                "getLeftOuter() can only be used for left or full outer join");
        checkArgument(batchSize > 0, "batchSize must be positive");
        requireNonNull(pixelsWriter, "pixelsWriter is null");

        List<Tuple> leftOuterTuples = new ArrayList<>();
        for (Tuple small : this.sortedSmallTable)
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

    @Override
    public int getSmallTableSize()
    {
        return this.sortedSmallTable.size();
    }

}
