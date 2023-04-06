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
package io.pixelsdb.pixels.turbo.planner.plan.logical;

import com.google.common.collect.ImmutableList;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.turbo.planner.plan.logical.Table.TableType.BASE;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 30/05/2022
 */
public class JoinGraph
{
    private final Set<Table> baseTables = new HashSet<>();
    private final List<Join> joins;
    private final Map<Table, List<Join>> joinIndex = new HashMap<>();

    public JoinGraph(List<Join> joins)
    {
        requireNonNull(joins, "joins is null");
        checkArgument(!joins.isEmpty(), "joins is empty");
        this.joins = ImmutableList.copyOf(joins);
        for (Join join : this.joins)
        {
            Table left = join.getLeftTable();
            Table right = join.getRightTable();
            checkArgument(left.getTableType() == BASE,
                    "left table in the link is not base table");
            checkArgument(right.getTableType() == BASE,
                    "right table in the link is not base table");
            this.baseTables.add(left);
            this.baseTables.add(right);
            if (this.joinIndex.containsKey(left))
            {
                this.joinIndex.get(left).add(join);
            }
            else
            {
                List<Join> joinList = new ArrayList<>();
                joinList.add(join);
                this.joinIndex.put(left, joinList);
            }
            if (this.joinIndex.containsKey(right))
            {
                this.joinIndex.get(right).add(join);
            }
            else
            {
                List<Join> joinList = new ArrayList<>();
                joinList.add(join);
                this.joinIndex.put(right, joinList);
            }
        }
    }

    public Set<Table> getBaseTables()
    {
        return baseTables;
    }

    public List<Join> getJoins()
    {
        return joins;
    }

    /**
     * Get the join plan which is a left-deep tree of the joins.
     * TODO: test and debug.
     *
     * @param comparator the comparator to order the tables.
     * @return the root joined table
     */
    public JoinedTable getJoinRoot(Comparator<Table> comparator)
    {
        JoinedTable joinRoot = null;
        List<Table> tableOrder = new ArrayList<>(this.baseTables);
        Collections.sort(tableOrder, comparator);
        Set<Table> visited = new HashSet<>();
        visited.add(tableOrder.get(0));
        while (visited.size() < tableOrder.size())
        {
            // get the candidate tables that are directly connected (joined) with the visited tables.
            Set<Table> candidate = new TreeSet<>(comparator);
            Map<Table, Join> linkMap = new HashMap<>();
            for (Table table : visited)
            {
                checkArgument(this.joinIndex.containsKey(table),
                        "table does not exist in the index");
                List<Join> joins = this.joinIndex.get(table);
                for (Join join : joins)
                {
                    Table left = join.getLeftTable();
                    Table right = join.getRightTable();
                    if (!visited.contains(left))
                    {
                        candidate.add(left);
                        linkMap.put(left, join);
                    }
                    if (!visited.contains(right))
                    {
                        candidate.add(right);
                        linkMap.put(right, join);
                    }
                }
            }
            // select the first table in the candidates as the next table to join.
            Iterator<Table> iterator = candidate.iterator();
            if (iterator.hasNext())
            {
                Table nextTable = iterator.next();
                visited.add(nextTable);
                Join baseJoin = linkMap.get(nextTable);
                Join newJoin;
                if (joinRoot == null)
                {
                    Table left = baseJoin.getLeftTable();
                    int[] leftKeyColumnIds = baseJoin.getLeftKeyColumnIds();
                    Table right = baseJoin.getRightTable();
                    int[] rightKeyColumnIds = baseJoin.getRightKeyColumnIds();
                    if (comparator.compare(left, right) > 0)
                    {
                        Table tmp = left;
                        left = right;
                        right = tmp;
                        int[] tmpInts = leftKeyColumnIds;
                        leftKeyColumnIds = rightKeyColumnIds;
                        rightKeyColumnIds = tmpInts;
                    }
                    // TODO: select join algorithm by the optimizer, decide includeKeyColumns and join endian.
                    newJoin = new Join(left, right,
                            left.getColumnNames(), right.getColumnNames(), leftKeyColumnIds, rightKeyColumnIds,
                            baseJoin.getLeftProjection(), baseJoin.getRightProjection(),
                            JoinEndian.SMALL_LEFT, baseJoin.getJoinType(), baseJoin.getJoinAlgo());
                }
                else
                {
                    JoinedTable left = joinRoot;
                    Table right = nextTable;
                    String[] leftColumns = left.getColumnNames();
                    boolean[] leftProjection = new boolean[leftColumns.length];
                    for (int i = 0; i < leftProjection.length; ++i)
                    {
                        leftProjection[i] = true;
                    }
                    int[] leftKeyColumnIds = new int[baseJoin.getLeftKeyColumnIds().length];
                    for (int i = 0; i < baseJoin.getLeftKeyColumnIds().length; ++i)
                    {
                        String leftKeyColum = baseJoin.getLeftTable().getColumnNames()
                                [baseJoin.getLeftKeyColumnIds()[i]];
                        for (int j = 0; j < leftColumns.length; ++j)
                        {
                            if (leftColumns[j].equals(leftKeyColum))
                            {
                                leftKeyColumnIds[i] = j;
                                break;
                            }
                        }
                    }
                    for (int leftKeyColumnId : leftKeyColumnIds)
                    {
                        leftProjection[leftKeyColumnId] = false;
                    }
                    int[] rightKeyColumnIds = baseJoin.getRightKeyColumnIds();
                    // TODO: select join algorithm by the optimizer, decide includeKeyColumns and join endian.
                    newJoin = new Join(left, right, left.getColumnNames(), right.getColumnNames(),
                            leftKeyColumnIds, rightKeyColumnIds, leftProjection, baseJoin.getRightProjection(),
                            JoinEndian.SMALL_LEFT, baseJoin.getJoinType(), baseJoin.getJoinAlgo());
                }

                String joinedSchemaName = "join_" + UUID.randomUUID().toString().replace("-", "");
                String joinedTableName = newJoin.getLeftTable().getTableName() + "_join_" +
                        newJoin.getRightTable().getTableName();

                joinRoot = new JoinedTable(joinedSchemaName, joinedTableName, joinedTableName, newJoin);
            }
            else
            {
                break;
            }
        }

        return joinRoot;
    }
}
