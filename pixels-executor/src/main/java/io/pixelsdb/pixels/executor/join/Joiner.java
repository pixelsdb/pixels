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

import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Use one set of row batches to build a hash map, and join it
 * with the other set of row batches.
 *
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
    private final int numKeyColumns;
    private final int batchSize;
    private VectorizedRowBatch joinedRowBatch;

    /**
     * The joiner to join two tables. Currently, only INNER/LEFT/RIGHT equi-joins are
     * supported. The first numKeyColumns columns in the two tables are considered as
     * the join key. The small table is used for hash probing.
     * <p/>
     * In the join result, the columns from the big table is at the beginning, and is
     * followed by the non-key columns from the small table.
     *
     * @param joinType the join type
     * @param smallPrefix the prefix for the columns from the small table, e.g., "orders."
     * @param smallSchema the schema of the small table
     * @param bigPrefix the prefix for the columns from the big table, e.e., "lineitem."
     * @param bigSchema the schema of the big table
     * @param numKeyColumns the number of key columns
     * @param batchSize the row-batch size of the input row batches of the small and big table
     *                  and the joined result row batches.
     */
    public Joiner(JoinType joinType, String smallPrefix, TypeDescription smallSchema,
                  String bigPrefix, TypeDescription bigSchema,
                  int numKeyColumns, int batchSize)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.smallSchema = requireNonNull(smallSchema, "smallSchema is null");
        this.bigSchema = requireNonNull(bigSchema, "bigSchema is null");
        checkArgument(numKeyColumns > 0, "numKeyColumns must be positive");
        this.numKeyColumns = numKeyColumns;
        checkArgument(batchSize > 0, "batchSize must be positive");
        this.batchSize = batchSize;
        // build the schema for the join result.
        this.joinedSchema = new TypeDescription(TypeDescription.Category.STRUCT);
        List<String> bigColumnNames = bigSchema.getFieldNames();
        List<TypeDescription> bigColumnTypes = bigSchema.getChildren();
        checkArgument(bigColumnTypes != null && bigColumnNames.size() == bigColumnTypes.size(),
                "invalid children of bigSchema");
        for (int i = 0; i < bigColumnNames.size(); ++i)
        {
            this.joinedSchema.addField(bigPrefix.concat(bigColumnNames.get(i)), bigColumnTypes.get(i));
        }
        List<String> smallColumnNames = smallSchema.getFieldNames();
        List<TypeDescription> smallColumnTypes = smallSchema.getChildren();
        checkArgument(smallColumnTypes != null && smallColumnNames.size() == smallColumnTypes.size(),
                "invalid children of smallSchema");
        // duplicate the join key if not nature join.
        for (int i = joinType == JoinType.NATURE ? numKeyColumns : 0; i < smallColumnNames.size(); ++i)
        {
            this.joinedSchema.addField(smallPrefix.concat(smallColumnNames.get(i)), smallColumnTypes.get(i));
        }
        this.joinedRowBatch = this.joinedSchema.createRowBatch();
    }

    public void populateSmall(VectorizedRowBatch smallBatch)
    {
        // TODO: implement.
    }

    public VectorizedRowBatch join(VectorizedRowBatch bigBatch)
    {
        // TODO: implement.
        return this.joinedRowBatch;
    }
}
