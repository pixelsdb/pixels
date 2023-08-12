/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.amphi.analyzer;

import io.pixelsdb.pixels.common.exception.AmphiException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * PlanAnalysis consumes and analyzes the logical plan (RelNode).
 */
public class PlanAnalysis
{
    private final String sql;
    private final RelNode root;

    private final MetadataService metadataService;
    private final String schemaName;

    private int nodeCount = 0;
    private int maxDepth = 0;

    private Set<String> scannedTables = new HashSet<>();
    private Set<String> operatorTypes = new HashSet<>();
    private Map<String, List<String>> projectColumns = new HashMap<>();

    private List<Map<String, Object>> filterDetails = new ArrayList<>();
    private List<Map<String, Object>> joinDetails = new ArrayList<>();
    private List<Map<String, Object>> aggregateDetails = new ArrayList<>();

    public PlanAnalysis(MetadataService instance, String sql, RelNode root, String schemaName)
    {
        this.sql = sql;
        this.root = root;
        this.metadataService = instance;
        this.schemaName = schemaName;
    }

    public void analyze() throws AmphiException, IOException, InterruptedException, MetadataException
    {
        traversePlan();
        sqlglotAnalyze();
    }

    /**
     * One-time traversal to collect all the required analysis factors.
     */
    public void traversePlan()
    {
        Consumer<RelNode> nodeCounter = (node) -> nodeCount++;

        Consumer<RelNode> depthCalculator = (node) -> {
            int depth = 0;
            RelNode currentNode = node;
            while (currentNode != null) {
                depth++;
                if (currentNode.getInputs().isEmpty()) {
                    currentNode = null;
                } else {
                    currentNode = currentNode.getInput(0);
                }
            }
            maxDepth = Math.max(maxDepth, depth);
        };

        Consumer<RelNode> filterDetailsCollector = (node) -> {
            if (node instanceof Filter) {
                Filter filter = (Filter) node;
                Map<String, Object> filterInfo = new HashMap<>();
                filterInfo.put("FilterCondition", filter.getCondition().toString());

                if (filter.getInput() instanceof TableScan) {
                    filterInfo.put("Table", ((TableScan) filter.getInput()).getTable().getQualifiedName().get(1));
                }

                filterDetails.add(filterInfo);
            }
        };

        Consumer<RelNode> joinDetailsCollector = (node) -> {
            if (node instanceof Join) {
                Join join = (Join) node;
                Map<String, Object> joinInfo = new HashMap<>();
                joinInfo.put("JoinType", join.getJoinType());
                joinInfo.put("JoinCondition", join.getCondition().toString());

                if (join.getLeft() instanceof TableScan && join.getRight() instanceof TableScan) {
                    joinInfo.put("LeftTable", ((TableScan) join.getLeft()).getTable().getQualifiedName().get(1));
                    joinInfo.put("RightTable", ((TableScan) join.getRight()).getTable().getQualifiedName().get(1));
                }

                joinDetails.add(joinInfo);
            }
        };

        Consumer<RelNode> aggregateDetailsCollector = (node) -> {
            if (node instanceof Aggregate) {
                Aggregate aggregate = (Aggregate) node;
                Map<String, Object> aggregateInfo = new HashMap<>();
                List<String> functions = new ArrayList<>();
                for (AggregateCall call : aggregate.getAggCallList()) {
                    functions.add(call.getAggregation().getKind().toString());
                }
                aggregateInfo.put("AggregateFunctions", functions);
                aggregateInfo.put("GroupedFields", aggregate.getGroupSet().toList());

                if (aggregate.getInput() instanceof TableScan) {
                    aggregateInfo.put("Table", ((TableScan) aggregate.getInput()).getTable().getQualifiedName().get(1));
                }

                aggregateDetails.add(aggregateInfo);
            }
        };

        Consumer<RelNode> scannedTableCollector = (node) -> {
            if (node instanceof TableScan) {
                TableScan tableScan = (TableScan) node;
                String table = tableScan.getTable().getQualifiedName().get(1);
                scannedTables.add(table);
            }
        };

        Consumer<RelNode> operatorTypeCollector = (node) -> operatorTypes.add(node.getRelTypeName());

        Consumer<RelNode>[] functions = new Consumer[]{
                nodeCounter,
                depthCalculator,
                scannedTableCollector,
                operatorTypeCollector,
                filterDetailsCollector,
                joinDetailsCollector,
                aggregateDetailsCollector
        };

        RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                for (Consumer<RelNode> function : functions) {
                    function.accept(node);
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(root);
    }

    /**
     * Leverage sqlglot package to parse high-level features of sql query.
     * @throws AmphiException
     * @throws IOException
     * @throws InterruptedException
     * @throws MetadataException
     */
    public void sqlglotAnalyze() throws AmphiException, IOException, InterruptedException, MetadataException
    {
        SqlglotExecutor executor = new SqlglotExecutor();
        List<String> columnList;
        Set<String> columnSet;

        // Retrieve the raw column fields
        columnList = executor.parseColumnFields(this.sql);
        columnSet = columnList.stream().collect(Collectors.toSet());

        // Validate the column fields with metadata
        List<Table> tables = metadataService.getTables(this.schemaName);
        for (Table table : tables)
        {
            List<String> columnsInTable = new ArrayList<>();
            List<Column> tableColumns = this.metadataService.getColumns(this.schemaName, table.getName(), false);
            for (Column column : tableColumns) {
                if (columnSet.contains(column.getName())) {
                    columnsInTable.add(column.getName());
                }
            }
            this.projectColumns.put(table.getName(), columnsInTable);
        }
    }

    public RelNode getRoot()
    {
        return this.root;
    }

    public int getNodeCount()
    {
        return this.nodeCount;
    }

    public int getMaxDepth()
    {
        return this.maxDepth;
    }

    public Set<String> getScannedTables()
    {
        return this.scannedTables;
    }

    public int getScannedTableCount()
    {
        return this.scannedTables.size();
    }

    public Set<String> getOperatorTypes()
    {
        return operatorTypes;
    }

    public List<Map<String, Object>> getFilterDetails()
    {
        return this.filterDetails;
    }

    public List<Map<String, Object>> getJoinDetails()
    {
        return this.joinDetails;
    }

    public List<Map<String, Object>> getAggregateDetails()
    {
        return this.aggregateDetails;
    }

    public Map<String, List<String>> getProjectColumns()
    {
        return projectColumns;
    }

}
