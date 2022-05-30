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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 30/05/2022
 */
public class JoinGraph
{
    private final Set<Table> baseTables = new HashSet<>();
    private final List<JoinLink> joinLinks;
    private final Map<Table, List<JoinLink>> linkIndex = new HashMap<>();

    public JoinGraph(List<JoinLink> joinLinks)
    {
        requireNonNull(joinLinks, "joinLinks is null");
        checkArgument(!joinLinks.isEmpty(), "joinLinks is empty");
        this.joinLinks = ImmutableList.copyOf(joinLinks);
        for (JoinLink link : this.joinLinks)
        {
            Table left = link.getLeftTable();
            Table right = link.getRightTable();
            checkArgument(left.isBase(), "left table in the link is not base table");
            checkArgument(right.isBase(), "right table in the link is not base table");
            this.baseTables.add(left);
            this.baseTables.add(right);
            if (this.linkIndex.containsKey(left))
            {
                this.linkIndex.get(left).add(link);
            }
            else
            {
                List<JoinLink> joins = new ArrayList<>();
                joins.add(link);
                this.linkIndex.put(left, joins);
            }
            if (this.linkIndex.containsKey(right))
            {
                this.linkIndex.get(right).add(link);
            }
            else
            {
                List<JoinLink> joins = new ArrayList<>();
                joins.add(link);
                this.linkIndex.put(right, joins);
            }
        }
    }

    public Set<Table> getBaseTables()
    {
        return baseTables;
    }

    public List<JoinLink> getJoinLinks()
    {
        return joinLinks;
    }

    /**
     * Get the join plan which is a left-deep tree of the joins.
     * TODO: test and debug.
     *
     * @param comparator the comparator to order the tables.
     * @return the join plan
     */
    public List<JoinedTable> getJoinPlan(Comparator<Table> comparator)
    {
        ImmutableList.Builder<JoinedTable> planBuilder = ImmutableList.builder();
        JoinedTable lastJoined = null;
        List<Table> tableOrder = new ArrayList<>(this.baseTables);
        Collections.sort(tableOrder, comparator);
        Set<Table> visited = new HashSet<>();
        visited.add(tableOrder.get(0));
        while (visited.size() < tableOrder.size())
        {
            // get the candidate tables that are directly connected (joined) with the visited tables.
            Set<Table> candidate = new TreeSet<>(comparator);
            Map<Table, JoinLink> linkMap = new HashMap<>();
            for (Table table : visited)
            {
                checkArgument(this.linkIndex.containsKey(table),
                        "table does not exist in the index");
                List<JoinLink> joins = this.linkIndex.get(table);
                for (JoinLink join : joins)
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
                JoinLink baseJoin = linkMap.get(nextTable);
                JoinLink newJoin;
                if (lastJoined == null)
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
                    // TODO: select join algorithm by the optimizer.
                    newJoin = new JoinLink(left, right, leftKeyColumnIds, rightKeyColumnIds,
                            baseJoin.getJoinType(), baseJoin.getJoinAlgo());
                }
                else
                {
                    JoinedTable left = lastJoined;
                    Table right = nextTable;
                    String[] leftColumns = left.getColumnNames();
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
                    int[] rightKeyColumnIds = baseJoin.getRightKeyColumnIds();
                    // TODO: select join algorithm by the optimizer.
                    newJoin = new JoinLink(left, right, leftKeyColumnIds, rightKeyColumnIds,
                            baseJoin.getJoinType(), baseJoin.getJoinAlgo());
                }
                String[] joinedColumns = ObjectArrays.concat(newJoin.getLeftTable().getColumnNames(),
                        newJoin.getRightTable().getColumnNames(), String.class);
                String joinedSchemaName = "join_" + UUID.randomUUID().toString().replace("-", "");
                String joinedTableName = newJoin.getLeftTable().getTableName() + "_join_" +
                        newJoin.getRightTable().getTableName();
                lastJoined = new JoinedTable(joinedSchemaName, joinedTableName, joinedTableName,
                        joinedColumns, newJoin);
                planBuilder.add(lastJoined);
            }
            else
            {
                break;
            }
        }

        return planBuilder.build();
    }
}
