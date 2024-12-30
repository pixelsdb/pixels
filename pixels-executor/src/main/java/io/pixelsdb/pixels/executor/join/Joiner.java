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
import io.pixelsdb.pixels.executor.utils.HashTable;
import io.pixelsdb.pixels.executor.utils.Tuple;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.executor.utils.NullTuple.buildNullTuple;
import static java.util.Objects.requireNonNull;

/**
 * Joiner uses a set of row batches from the small table to build a hash table, and join it
 * with the row batches from the large table. In this Joiner, we <b>assume</b> that the left
 * table is the small table, and the right table is the large table.
 * <p/>
 * <b>Note 1:</b> this joiner can not be directly used for left outer broadcast distributed
 * join, because it does not ensure that the unmatched tuples from the small table will be
 * returned only once by all the joiners within the join.
 * <p/>
 * <b>Note 2:</b> only equi-joins are supported. Semi-join and anti-join are not supported.
 * Thus, null values are not checked in the join. There is no join result if either or both
 * side(s) is(are) null.
 *
 * @author hank
 * @date 07/05/2022
 */
public abstract class Joiner
{
    protected final JoinType joinType;
    protected final TypeDescription smallSchema;
    protected final TypeDescription largeSchema;
    protected final TypeDescription joinedSchema;
    protected final int[] smallKeyColumnIds;
    protected final int[] largeKeyColumnIds;
    /**
     * Whether the columns from {@link #smallSchema} should be included in the {@link #joinedSchema}.
     */
    protected final boolean[] smallProjection;
    /**
     * Whether the columns from {@link #largeSchema} should be included in the {@link #joinedSchema}.
     */
    protected final boolean[] largeProjection;
    /**
     * All the tuples from the small table that have been matched during the join.
     */
    protected final Set<Tuple> matchedSmallTuples = new HashSet<>();
    protected final Tuple smallNullTuple;
    protected final Tuple largeNullTuple;

    /**
     * The joiner to join two tables. Currently, only NATURE/INNER/LEFT/RIGHT equi-joins
     * are supported. The first numKeyColumns columns in the two tables are considered as
     * the join key. The small table, a.k.a., the left table, is hash probed in the join.
     * <p/>
     * In the join result, first comes with the columns from the small table, followed by
     * the columns from the large (a.k.a., right) table.
     *
     * @param joinType the join type
     * @param smallSchema the schema of the small table
     * @param smallColumnAlias the alias of the columns from the small table. These alias
     *                         are used as the column name in {@link #joinedSchema}
     * @param smallProjection denotes whether the columns from {@link #smallSchema}
     *                        are included in {@link #joinedSchema}
     * @param smallKeyColumnIds the ids of the key columns of the small table
     * @param largeSchema the schema of the large table
     * @param largeColumnAlias the alias of the columns from the large table. These alias
     *                         are used as the column name in {@link #joinedSchema}
     * @param largeProjection denotes whether the columns from {@link #largeSchema}
     *                        are included in {@link #joinedSchema}
     * @param largeKeyColumnIds the ids of the key columns of the large table
     */
    public Joiner(JoinType joinType,
                  TypeDescription smallSchema, String[] smallColumnAlias,
                  boolean[] smallProjection, int[] smallKeyColumnIds,
                  TypeDescription largeSchema, String[] largeColumnAlias,
                  boolean[] largeProjection, int[] largeKeyColumnIds)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
        requireNonNull(smallColumnAlias, "smallColumnAlias is null");
        requireNonNull(largeColumnAlias, "largeColumnAlias is null");
        checkArgument(joinType != JoinType.UNKNOWN, "joinType is UNKNOWN");
        this.smallSchema = requireNonNull(smallSchema, "smallSchema is null");
        this.largeSchema = requireNonNull(largeSchema, "largeSchema is null");
        checkArgument(smallKeyColumnIds != null && smallKeyColumnIds.length > 0,
                "smallKeyColumnIds is null or empty");
        checkArgument(largeKeyColumnIds != null && largeKeyColumnIds.length > 0,
                "largeKeyColumnIds is null or empty");
        checkArgument(smallProjection != null && smallProjection.length == smallSchema.getFieldNames().size(),
                "smallProjection is null or of incorrect length");
        checkArgument(largeProjection != null && largeProjection.length == largeSchema.getFieldNames().size(),
                "largeProjection is null or of incorrect length");

        this.smallKeyColumnIds = smallKeyColumnIds;
        this.smallProjection = smallProjection;
        int numSmallIncludedColumns = 0;
        for (boolean smallProj : this.smallProjection)
        {
            if (smallProj)
            {
                ++numSmallIncludedColumns;
            }
        }
        checkArgument(smallColumnAlias.length == numSmallIncludedColumns,
                "smallProjection is not consist with smallColumnAlias");

        this.largeKeyColumnIds = largeKeyColumnIds;
        this.largeProjection = largeProjection;
        int numLargeIncludedColumns = 0;
        for (boolean largeProj : this.largeProjection)
        {
            if (largeProj)
            {
                ++numLargeIncludedColumns;
            }
        }
        checkArgument(largeColumnAlias.length == numLargeIncludedColumns,
                "largeProjection is not consist with largeColumnAlias");

        // build the schema for the join result.
        this.joinedSchema = new TypeDescription(TypeDescription.Category.STRUCT);
        List<String> smallColumnNames = smallSchema.getFieldNames();
        List<TypeDescription> smallColumnTypes = smallSchema.getChildren();
        checkArgument(smallColumnTypes != null && smallColumnNames.size() == smallColumnTypes.size(),
                "invalid children of smallSchema");
        List<String> largeColumnNames = largeSchema.getFieldNames();
        List<TypeDescription> largeColumnTypes = largeSchema.getChildren();
        checkArgument(largeColumnTypes != null && largeColumnNames.size() == largeColumnTypes.size(),
                "invalid children of largeSchema");

        int colAliasId = 0;
        for (int i = 0; i < smallColumnNames.size(); ++i)
        {
            if (this.smallProjection[i])
            {
                this.joinedSchema.addField(smallColumnAlias[colAliasId++], smallColumnTypes.get(i));
            }
        }
        colAliasId = 0;
        for (int i = 0; i < largeColumnNames.size(); ++i)
        {
            if (this.largeProjection[i])
            {
                this.joinedSchema.addField(largeColumnAlias[colAliasId++], largeColumnTypes.get(i));
            }
        }
        // create the null tuples for outer join.
        this.smallNullTuple = buildNullTuple(numSmallIncludedColumns);
        this.largeNullTuple = buildNullTuple(numLargeIncludedColumns);
    }

    public TypeDescription getJoinedSchema()
    {
        return this.joinedSchema;
    }

    public abstract List<VectorizedRowBatch> join(VectorizedRowBatch largeBatch);

    public abstract void populateLeftTable(VectorizedRowBatch smallBatch);

    public abstract boolean writeLeftOuter(PixelsWriter pixelsWriter, int batchSize) throws IOException;

    public abstract int getSmallTableSize();
}
