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
package io.pixelsdb.pixels.executor.plan;

import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * A join pipeline is a list of tables that are joined from left to right.
 * For example, the pipeline [customer, orders, lineitem, supplier] means the
 * tables are joined as (((customer join orders) join lineitem) join supplier).
 *
 * @author hank
 * @date 26/05/2022
 */
public class JoinPipeline extends Table
{
    /**
     * The N tables in the pipeline.
     */
    private final List<Table> tables;
    /**
     * The N-1 join types in the pipeline.
     * There is a join type for each pair of adjacent tables in the pipeline.
     */
    private List<JoinType> joinTypes;
    /**
     * The N-1 join algorithms in the pipeline.
     * There is a join type for each pair of adjacent tables in the pipeline.
     */
    private List<JoinAlgorithm> joinAlgos;

    public JoinPipeline(boolean isBase, String schemaName, String tableName, int[] keyColumnIds, String[] includeCols,
                        TableScanFilter filter, Table left, Table right, JoinType joinType, JoinAlgorithm joinAlgo)
    {
        super(isBase, schemaName, tableName, keyColumnIds, includeCols, filter);
        this.tables = new ArrayList<>();
        this.tables.add(left);
        this.tables.add(right);
        this.joinTypes = new ArrayList<>();
        this.joinTypes.add(joinType);
        this.joinAlgos = new ArrayList<>();
        this.joinAlgos.add(joinAlgo);
    }

    public void addTable(Table table, JoinType joinType, JoinAlgorithm joinAlgo)
    {
        this.tables.add(table);
        this.joinTypes.add(joinType);
        this.joinAlgos.add(joinAlgo);
    }
}
