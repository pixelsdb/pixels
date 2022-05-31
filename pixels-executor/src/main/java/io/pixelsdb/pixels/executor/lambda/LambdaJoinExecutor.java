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
package io.pixelsdb.pixels.executor.lambda;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.metadata.domain.Projections;
import io.pixelsdb.pixels.common.metadata.domain.Splits;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.plan.BaseTable;
import io.pixelsdb.pixels.executor.plan.JoinLink;
import io.pixelsdb.pixels.executor.plan.JoinedTable;
import io.pixelsdb.pixels.executor.plan.Table;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The executor of lambda-based joins.
 *
 * @author hank
 * @date 07/05/2022
 */
public class LambdaJoinExecutor
{
    private static final Logger logger = LogManager.getLogger(LambdaJoinExecutor.class);
    private final List<JoinedTable> joinPlan;
    private final ConfigFactory config;
    private final MetadataService metadataService;
    private final int fixedSplitSize;
    private final boolean projectionReadEnabled;
    private final boolean orderedPathEnabled;
    private final boolean compactPathEnabled;
    private final Storage storage;
    private final long queryId;

    public LambdaJoinExecutor(long queryId,
                              List<JoinedTable> joinPlan,
                              boolean orderedPathEnabled,
                              boolean compactPathEnabled)
            throws IOException
    {
        this.queryId = queryId;
        this.joinPlan = requireNonNull(joinPlan, "joinPlan is null");
        this.config = ConfigFactory.Instance();
        this.metadataService = new MetadataService(config.getProperty("metadata.server.host"),
                Integer.parseInt(config.getProperty("metadata.server.port")));
        this.fixedSplitSize = Integer.parseInt(config.getProperty("fixed.split.size"));
        this.projectionReadEnabled = Boolean.parseBoolean(config.getProperty("projection.read.enabled"));
        this.orderedPathEnabled = orderedPathEnabled;
        this.compactPathEnabled = compactPathEnabled;
        this.storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
    }

    private void execute() throws IOException, MetadataException, ExecutionException, InterruptedException
    {
        List<JoinOutput> prevJoinOutputs = null;
        for (JoinedTable joinedTable : joinPlan)
        {
            JoinLink join = joinedTable.getJoinLink();
            Table leftTable = join.getLeftTable();
            BaseTable rightTable = (BaseTable) join.getRightTable();
            JoinAlgorithm joinAlgo = join.getJoinAlgo();
            List<ScanInput.InputInfo> leftInputs = new ArrayList<>();
            int leftSplitSize = 0;
            if (leftTable.isBase())
            {
                leftSplitSize = getInputs(leftTable, leftInputs);
            }
            else
            {
                requireNonNull(prevJoinOutputs, "the previous join outputs is null");
                for (JoinOutput joinOutput : prevJoinOutputs)
                {
                    for (int i = 0; i < joinOutput.getOutputs().size(); ++i)
                    {
                        ScanInput.InputInfo input = new ScanInput.InputInfo(
                                joinOutput.getOutputs().get(i),
                                0, joinOutput.getRowGroupNums().get(i));
                        leftInputs.add(input);
                        leftSplitSize += joinOutput.getRowGroupNums().get(i);
                    }
                }
                leftSplitSize /= leftInputs.size();
                if (leftSplitSize <= 0)
                {
                    leftSplitSize = 1;
                }
            }
            List<ScanInput.InputInfo> rightInputs = new ArrayList<>();
            int rightSplitSize = getInputs(rightTable, rightInputs);

            if (joinAlgo == JoinAlgorithm.BROADCAST)
            {
                List<BroadcastJoinInput> joinInputs = new ArrayList<>();
                BroadcastJoinInput.TableInfo leftTableInfo = new BroadcastJoinInput.TableInfo();
                leftTableInfo.setTableName(leftTable.getTableName());
                leftTableInfo.setInputs(leftInputs);
                leftTableInfo.setCols(leftTable.getColumnNames());
                leftTableInfo.setSplitSize(leftSplitSize);
                leftTableInfo.setFilter(JSON.toJSONString(
                        TableScanFilter.empty(leftTable.getSchemaName(), leftTable.getTableName())));
                leftTableInfo.setKeyColumnIds(join.getRightKeyColumnIds());
                for (int i = 0; i < rightInputs.size(); ++i)
                {
                    List<ScanInput.InputInfo> inputs = new ArrayList<>();
                    for (int j = 0; j < rightSplitSize && i < rightInputs.size(); ++i)
                    {
                        inputs.add(rightInputs.get(i));
                        j += rightInputs.get(i).getRgLength();
                    }

                    BroadcastJoinInput.TableInfo rightTableInfo = new BroadcastJoinInput.TableInfo();
                    rightTableInfo.setTableName(rightTable.getTableName());
                    rightTableInfo.setInputs(inputs);
                    rightTableInfo.setCols(rightTable.getColumnNames());
                    rightTableInfo.setSplitSize(rightSplitSize);
                    rightTableInfo.setFilter(JSON.toJSONString(rightTable.getFilter()));
                    rightTableInfo.setKeyColumnIds(join.getRightKeyColumnIds());
                    ScanInput.OutputInfo output = new ScanInput.OutputInfo();
                    output.setEncoding(true);
                    output.setEndpoint(Storage.Scheme.s3.name());
                    output.setFolder("pixels-lambda/" + joinedTable.getTableName());
                    BroadcastJoinInput joinInput = new BroadcastJoinInput();
                    joinInput.setQueryId(queryId);
                    joinInput.setLeftTable(leftTableInfo);
                    joinInput.setRightTable(rightTableInfo);
                    joinInput.setJoinedCols(joinedTable.getColumnNames());
                    joinInput.setJoinType(join.getJoinType());
                    joinInput.setOutput(output);
                    joinInputs.add(joinInput);
                }
                CompletableFuture<JoinOutput>[] joinOutputs = new CompletableFuture[joinInputs.size()];
                for (int i = 0; i < joinInputs.size(); ++i)
                {
                    joinOutputs[i] = BroadcastJoinInvoker.invoke(joinInputs.get(i));
                }
                CompletableFuture.allOf(joinOutputs).join();
                prevJoinOutputs = new ArrayList<>(joinOutputs.length);
                for (CompletableFuture<JoinOutput> joinOutput : joinOutputs)
                {
                    prevJoinOutputs.add(joinOutput.get());
                }
            }
            else if (joinAlgo == JoinAlgorithm.PARTITIONED)
            {
                int numRowGroups = 0;
                for (ScanInput.InputInfo input : rightInputs)
                {
                    numRowGroups += input.getRgLength();
                }
                // TODO: calculate numPartitions from configuration.
                int numPartition = numRowGroups / rightSplitSize / 6;
                String leftOutputBase = "pixels-lambda/" + joinedTable.getTableName() + "/" +
                        leftTable.getTableName() + "/part-";
                String rightOutputBase = "pixels-lambda/" + joinedTable.getTableName() + "/" +
                        rightTable.getTableName() + "/part-";
                List<PartitionInput> leftPartitionInputs = getPartitionInputs(numPartition,
                        leftInputs, leftSplitSize, leftTable, leftOutputBase, join.getLeftKeyColumnIds());
                List<PartitionInput> rightPartitionInputs = getPartitionInputs(numPartition,
                        rightInputs, rightSplitSize, rightTable, rightOutputBase, join.getRightKeyColumnIds());
                // TODO: continue implementation.
            }
        }
    }

    private List<PartitionInput> getPartitionInputs(
            int numPartition, List<ScanInput.InputInfo> inputs, int splitSize,
            Table inputTable, String outputBase, int[] keyColumnIds)
    {
        List<PartitionInput> partitionInputs = new ArrayList<>();
        for (int i = 0; i < inputs.size(); ++i)
        {
            List<ScanInput.InputInfo> splitInputs = new ArrayList<>();
            for (int j = 0; j < splitSize && i < inputs.size(); ++i)
            {
                splitInputs.add(inputs.get(i));
                j += inputs.get(i).getRgLength();
            }
            PartitionInput partitionInput = new PartitionInput();
            partitionInput.setQueryId(queryId);
            partitionInput.setInputs(splitInputs);
            partitionInput.setCols(inputTable.getColumnNames());
            if (inputTable.isBase())
            {
                partitionInput.setFilter(JSON.toJSONString(((BaseTable) inputTable).getFilter()));
            }
            else
            {
                partitionInput.setFilter(JSON.toJSONString(
                        TableScanFilter.empty(inputTable.getSchemaName(), inputTable.getTableName())));
            }
            partitionInput.setSplitSize(splitSize);
            partitionInput.setOutput(new PartitionInput.OutputInfo(outputBase + i, true));
            partitionInput.setPartitionInfo(
                    new PartitionInput.PartitionInfo(keyColumnIds,numPartition));
            partitionInputs.add(partitionInput);
        }

        return partitionInputs;
    }

    private int getInputs(Table table, List<ScanInput.InputInfo> inputs) throws MetadataException, IOException
    {
        requireNonNull(table, "table is null");
        checkArgument(table.isBase(), "this is not a base table");
        int splitSize = 0;
        List<Layout> layouts = metadataService.getLayouts(table.getSchemaName(), table.getTableName());
        for (Layout layout : layouts)
        {
            int version = layout.getVersion();
            IndexName indexName = new IndexName(table.getSchemaName(), table.getTableName());
            Order order = JSON.parseObject(layout.getOrder(), Order.class);
            ColumnSet columnSet = new ColumnSet();
            for (String column : table.getColumnNames())
            {
                columnSet.addColumn(column);
            }

            // get split size
            Splits splits = JSON.parseObject(layout.getSplits(), Splits.class);
            if (this.fixedSplitSize > 0)
            {
                splitSize = this.fixedSplitSize;
            }
            else
            {
                // log.info("columns to be accessed: " + columnSet.toString());
                SplitsIndex splitsIndex = IndexFactory.Instance().getSplitsIndex(indexName);
                if (splitsIndex == null)
                {
                    logger.debug("splits index not exist in factory, building index...");
                    splitsIndex = buildSplitsIndex(order, splits, indexName);
                }
                else
                {
                    int indexVersion = splitsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("splits index version is not up-to-date, updating index...");
                        splitsIndex = buildSplitsIndex(order, splits, indexName);
                    }
                }
                SplitPattern bestSplitPattern = splitsIndex.search(columnSet);
                // log.info("bestPattern: " + bestPattern.toString());
                splitSize = bestSplitPattern.getSplitSize();
            }
            logger.debug("using split size: " + splitSize);
            int rowGroupNum = splits.getNumRowGroupInBlock();

            // get compact path
            String compactPath;
            if (projectionReadEnabled)
            {
                ProjectionsIndex projectionsIndex = IndexFactory.Instance().getProjectionsIndex(indexName);
                Projections projections = JSON.parseObject(layout.getProjections(), Projections.class);
                if (projectionsIndex == null)
                {
                    logger.debug("projections index not exist in factory, building index...");
                    projectionsIndex = buildProjectionsIndex(order, projections, indexName);
                }
                else
                {
                    int indexVersion = projectionsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("projections index is not up-to-date, updating index...");
                        projectionsIndex = buildProjectionsIndex(order, projections, indexName);
                    }
                }
                ProjectionPattern projectionPattern = projectionsIndex.search(columnSet);
                if (projectionPattern != null)
                {
                    logger.debug("suitable projection pattern is found, path='" + projectionPattern.getPath() + '\'');
                    compactPath = projectionPattern.getPath();
                }
                else
                {
                    compactPath = layout.getCompactPath();
                }
            }
            else
            {
                compactPath = layout.getCompactPath();
            }
            logger.debug("using compact path: " + compactPath);

            // get the inputs from storage
            try
            {
                // 1. add splits in orderedPath
                if (orderedPathEnabled)
                {
                    List<String> orderedPaths = storage.listPaths(layout.getOrderPath());

                    int numPath = orderedPaths.size();
                    for (int i = 0; i < numPath; ++i)
                    {
                        ScanInput.InputInfo input = new ScanInput.InputInfo(orderedPaths.get(i), 0 , 1);
                        inputs.add(input);
                    }
                }
                // 2. add splits in compactPath
                if (compactPathEnabled)
                {
                    List<String> compactPaths = storage.listPaths(compactPath);

                    int curFileRGIdx;
                    for (String path : compactPaths)
                    {
                        curFileRGIdx = 0;
                        while (curFileRGIdx < rowGroupNum)
                        {
                            ScanInput.InputInfo input = new ScanInput.InputInfo(path, curFileRGIdx, splitSize);
                            inputs.add(input);
                            curFileRGIdx += splitSize;
                        }
                    }
                }
            }
            catch (IOException e)
            {
                throw new IOException("failed to get input information from storage", e);
            }
        }

        return splitSize;
    }

    private SplitsIndex buildSplitsIndex(Order order, Splits splits, IndexName indexName) {
        List<String> columnOrder = order.getColumnOrder();
        SplitsIndex index;
        index = new InvertedSplitsIndex(columnOrder, SplitPattern.buildPatterns(columnOrder, splits),
                splits.getNumRowGroupInBlock());
        IndexFactory.Instance().cacheSplitsIndex(indexName, index);
        return index;
    }

    private ProjectionsIndex buildProjectionsIndex(Order order, Projections projections, IndexName indexName) {
        List<String> columnOrder = order.getColumnOrder();
        ProjectionsIndex index;
        index = new InvertedProjectionsIndex(columnOrder, ProjectionPattern.buildPatterns(columnOrder, projections));
        IndexFactory.Instance().cacheProjectionsIndex(indexName, index);
        return index;
    }
}
