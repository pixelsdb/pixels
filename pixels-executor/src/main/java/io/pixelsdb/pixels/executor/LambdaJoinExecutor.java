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
package io.pixelsdb.pixels.executor;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
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
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.lambda.JoinOperator;
import io.pixelsdb.pixels.executor.lambda.PartitionedJoinOperator;
import io.pixelsdb.pixels.executor.lambda.SingleStageJoinOperator;
import io.pixelsdb.pixels.executor.lambda.domain.*;
import io.pixelsdb.pixels.executor.lambda.input.*;
import io.pixelsdb.pixels.executor.plan.BaseTable;
import io.pixelsdb.pixels.executor.plan.Join;
import io.pixelsdb.pixels.executor.plan.JoinedTable;
import io.pixelsdb.pixels.executor.plan.Table;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    private final JoinedTable rootTable;
    private final ConfigFactory config;
    private final MetadataService metadataService;
    private final int fixedSplitSize;
    private final boolean projectionReadEnabled;
    private final boolean orderedPathEnabled;
    private final boolean compactPathEnabled;
    private final Storage storage;
    private final long queryId;

    public LambdaJoinExecutor(long queryId,
                              JoinedTable rootTable,
                              boolean orderedPathEnabled,
                              boolean compactPathEnabled)
            throws IOException
    {
        this.queryId = queryId;
        this.rootTable = requireNonNull(rootTable, "rootTable is null");
        this.config = ConfigFactory.Instance();
        this.metadataService = new MetadataService(config.getProperty("metadata.server.host"),
                Integer.parseInt(config.getProperty("metadata.server.port")));
        this.fixedSplitSize = Integer.parseInt(config.getProperty("fixed.split.size"));
        this.projectionReadEnabled = Boolean.parseBoolean(config.getProperty("projection.read.enabled"));
        this.orderedPathEnabled = orderedPathEnabled;
        this.compactPathEnabled = compactPathEnabled;
        this.storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
    }

    protected JoinOperator getJoinOperator(JoinedTable joinedTable, Optional<JoinedTable> parent)
            throws IOException, MetadataException
    {
        requireNonNull(joinedTable, "joinedTable is null");
        Join join = requireNonNull(joinedTable.getJoin(), "joinTable.join is null");
        Table leftTable = requireNonNull(join.getLeftTable(), "join.leftTable is null");
        requireNonNull(join.getRightTable(), "join.rightTable is null");
        checkArgument(join.getRightTable().isBase(), "join.rightTable is not base table");
        BaseTable rightTable = (BaseTable) join.getRightTable();
        int[] leftKeyColumnIds = requireNonNull(join.getLeftKeyColumnIds(),
                "join.leftKeyColumnIds is null");
        int[] rightKeyColumnIds = requireNonNull(join.getRightKeyColumnIds(),
                "join.rightKeyColumnIds is null");
        JoinType joinType = requireNonNull(join.getJoinType(), "join.joinType is null");
        JoinAlgorithm joinAlgo = requireNonNull(join.getJoinAlgo(), "join.joinAlgo is null");

        List<InputSplit> leftInputSplits = null;
        List<String> leftPartitionedFiles = null;
        // get the rightInputSplits from metadata.
        List<InputSplit> rightInputSplits = getInputSplits(rightTable);
        JoinOperator childOperator = null;

        if (leftTable.isBase())
        {
            // get the leftInputSplits from metadata.
            leftInputSplits = getInputSplits((BaseTable) leftTable);
            // check if there is a chain join.
            if (joinAlgo == JoinAlgorithm.BROADCAST && parent.isPresent() &&
                    parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST)
            {
                /*
                 * Chain join is found, and this is the first broadcast join in the chain.
                 * In this case, we build an incomplete chain join with only two left tables and one ChainJoinInfo.
                 */
                BroadCastJoinTableInfo leftTableInfo = getBroadcastJoinTableInfo(
                        leftTable, leftInputSplits, join.getLeftKeyColumnIds());
                BroadCastJoinTableInfo rightTableInfo = getBroadcastJoinTableInfo(
                        rightTable, rightInputSplits, join.getRightKeyColumnIds());
                ChainJoinInfo chainJoinInfo = new ChainJoinInfo(
                        joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                        parent.get().getJoin().getLeftKeyColumnIds(), join.isIncludeKeyColumns(),
                        false, null);

                ChainJoinInput chainJoinInput = new ChainJoinInput();
                chainJoinInput.setQueryId(queryId);
                List<BroadCastJoinTableInfo> leftTableInfos = new ArrayList<>();
                leftTableInfos.add(leftTableInfo);
                leftTableInfos.add(rightTableInfo);
                chainJoinInput.setSmallTables(leftTableInfos);
                List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();
                chainJoinInfos.add(chainJoinInfo);
                chainJoinInput.setChainJoinInfos(chainJoinInfos);

                return new SingleStageJoinOperator(chainJoinInput, JoinAlgorithm.CHAIN);
            }
        }
        else
        {
            childOperator = getJoinOperator((JoinedTable) leftTable, Optional.of(joinedTable));
            requireNonNull(childOperator, "failed to get child operator");
            // check if there is an incomplete chain join.
            if (childOperator.getJoinAlgo() == JoinAlgorithm.CHAIN && joinAlgo == JoinAlgorithm.BROADCAST)
            {
                // the current join is still a broadcast join, thus the left child is an incomplete chain join.
                if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST)
                {
                    /*
                     * The parent is still a broadcast join, continue chain join construction by
                     * adding the right table of the current join into the left tables of the chain join
                     */
                    BroadCastJoinTableInfo rightTableInfo = getBroadcastJoinTableInfo(
                            rightTable, rightInputSplits, join.getRightKeyColumnIds());
                    ChainJoinInfo chainJoinInfo = new ChainJoinInfo(
                            joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                            parent.get().getJoin().getLeftKeyColumnIds(), join.isIncludeKeyColumns(),
                            false, null);
                    checkArgument(childOperator.getJoinInputs().size() == 1,
                            "there should be exact one incomplete chain join input in the child operator");
                    ChainJoinInput chainJoinInput = (ChainJoinInput) childOperator.getJoinInputs().get(0);
                    chainJoinInput.getSmallTables().add(rightTableInfo);
                    chainJoinInput.getChainJoinInfos().add(chainJoinInfo);
                    // no need to create a new operator.
                    return childOperator;
                }
                else
                {
                    // The parent is not present or is not a broadcast join, complete chain join construction.
                    boolean postPartition = false;
                    PartitionInfo postPartitionInfo = null;
                    if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
                    {
                        postPartition = true;
                        // TODO: get numPartition from the optimizer.
                        postPartitionInfo = new PartitionInfo(
                                parent.get().getJoin().getLeftKeyColumnIds(), 40);
                    }
                    JoinInfo joinInfo = new JoinInfo(joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                            join.isIncludeKeyColumns(), postPartition, postPartitionInfo);

                    checkArgument(childOperator.getJoinInputs().size() == 1,
                            "there should be exact one incomplete chain join input in the child operator");
                    ChainJoinInput chainJoinInput = (ChainJoinInput) childOperator.getJoinInputs().get(0);

                    ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
                    int outputId = 0;
                    for (InputSplit rightInputSplit : rightInputSplits)
                    {
                        BroadCastJoinTableInfo rightTableInfo = getBroadcastJoinTableInfo(
                                rightTable, ImmutableList.of(rightInputSplit), join.getRightKeyColumnIds());

                        // TODO: get the path base from config file.
                        MultiOutputInfo output = new MultiOutputInfo(
                                "pixels-lambda/" + joinedTable.getTableName() + "/", Storage.Scheme.s3,
                                null, null, null, true,
                                ImmutableList.of("join_" + outputId++));

                        ChainJoinInput complete = chainJoinInput.toBuilder()
                                .setLargeTable(rightTableInfo)
                                .setJoinInfo(joinInfo)
                                .setOutput(output).build();

                        joinInputs.add(complete);
                    }
                    SingleStageJoinOperator joinOperator =
                            new SingleStageJoinOperator(joinInputs.build(), JoinAlgorithm.CHAIN);
                    return joinOperator;
                }
            }
            // get the leftInputSplits or leftPartitionedFiles from childJoinInputs.
            List<JoinInput> childJoinInputs = childOperator.getJoinInputs();
            if (joinAlgo == JoinAlgorithm.BROADCAST)
            {
                ImmutableList.Builder<InputSplit> inputSplits = ImmutableList.builder();
                for (JoinInput childJoinInput : childJoinInputs)
                {
                    MultiOutputInfo childOutput = childJoinInput.getOutput();
                    String base = childOutput.getPath();
                    if (!base.endsWith("/"))
                    {
                        base += "/";
                    }
                    ImmutableList.Builder<InputInfo> inputs = ImmutableList.builder();
                    for (String fileName : childOutput.getFileNames())
                    {
                        InputInfo inputInfo = new InputInfo(base + fileName, 0, -1);
                        inputs.add(inputInfo);
                    }
                    inputSplits.add(new InputSplit(inputs.build()));
                }
                leftInputSplits = inputSplits.build();
            }
            else if (joinAlgo == JoinAlgorithm.PARTITIONED)
            {
                ImmutableList.Builder<String> partitionedFiles = ImmutableList.builder();
                for (JoinInput childJoinInput : childJoinInputs)
                {
                    MultiOutputInfo childOutput = childJoinInput.getOutput();
                    String base = childOutput.getPath();
                    if (!base.endsWith("/"))
                    {
                        base += "/";
                    }
                    for (String fileName : childOutput.getFileNames())
                    {
                        partitionedFiles.add(base + fileName);
                    }
                }
                leftPartitionedFiles = partitionedFiles.build();
            }
            else
            {
                throw new UnsupportedOperationException("join algorithm '" + joinAlgo +
                        "' is not supported in the joined table constructed by users");
            }
        }

        // generate join inputs and return.
        if (joinAlgo == JoinAlgorithm.BROADCAST)
        {
            requireNonNull(leftInputSplits, "leftInputSplits is null");
            BroadCastJoinTableInfo leftTableInfo = getBroadcastJoinTableInfo(
                    leftTable, leftInputSplits, join.getLeftKeyColumnIds());

            boolean postPartition = false;
            PartitionInfo postPartitionInfo = null;
            if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
            {
                postPartition = true;
                // TODO: get numPartition from the optimizer.
                postPartitionInfo = new PartitionInfo(
                        parent.get().getJoin().getLeftKeyColumnIds(), 40);
            }
            JoinInfo joinInfo = new JoinInfo(joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                    join.isIncludeKeyColumns(), postPartition, postPartitionInfo);

            ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
            int outputId = 0;
            for (InputSplit rightInputSplit : rightInputSplits)
            {
                BroadCastJoinTableInfo rightTableInfo = getBroadcastJoinTableInfo(
                        rightTable, ImmutableList.of(rightInputSplit), join.getRightKeyColumnIds());

                // TODO: get the path base from config file.
                MultiOutputInfo output = new MultiOutputInfo(
                        "pixels-lambda/" + joinedTable.getTableName(), Storage.Scheme.s3,
                        null, null, null,true,
                        ImmutableList.of("join_" + outputId++));

                BroadcastJoinInput joinInput = new BroadcastJoinInput(
                        queryId, leftTableInfo, rightTableInfo, joinInfo, output);

                joinInputs.add(joinInput);
            }
            SingleStageJoinOperator joinOperator =
                    new SingleStageJoinOperator(joinInputs.build(), joinAlgo);
            joinOperator.setChild(childOperator);
            return joinOperator;
        }
        else if (joinAlgo == JoinAlgorithm.PARTITIONED)
        {
            // process partitioned join.
            PartitionedJoinOperator joinOperator;
            if (childOperator != null)
            {
                // left side is post partitioned, thus we only partition the right table.
                // TODO: get parallelism from config file.
                PartitionedTableInfo leftTableInfo = new PartitionedTableInfo(
                        leftTable.getTableName(), leftPartitionedFiles, 8,
                        leftTable.getColumnNames(), leftKeyColumnIds);

                // TODO: get numPartition and outputBase from optimizer and config file respectively.
                List<PartitionInput> rightPartitionInputs = getPartitionInputs(
                        rightTable, rightInputSplits, rightKeyColumnIds, 40,
                        "pixels-lambda/" + joinedTable.getTableName() + "/" +
                                rightTable.getTableName() + "/part-");

                PartitionedTableInfo rightTableInfo = getPartitionedTableInfo(
                        rightTable, rightKeyColumnIds, rightPartitionInputs);

                List<JoinInput> joinInputs = getPartitionedJoinInputs(
                        joinedTable, parent, leftTableInfo, rightTableInfo);

                joinOperator = new PartitionedJoinOperator(
                        null, rightPartitionInputs, joinInputs, joinAlgo);
            }
            else
            {
                // partition both tables. in this case, the operator's child must be null.
                // TODO: get numPartition and outputBase from optimizer and config file respectively.
                List<PartitionInput> leftPartitionInputs = getPartitionInputs(
                        leftTable, leftInputSplits, leftKeyColumnIds, 40,
                        "pixels-lambda/" + joinedTable.getTableName() + "/" +
                                leftTable.getTableName() + "/part-");
                PartitionedTableInfo leftTableInfo = getPartitionedTableInfo(
                        leftTable, leftKeyColumnIds, leftPartitionInputs);

                // TODO: get numPartition and outputBase from optimizer and config file respectively.
                List<PartitionInput> rightPartitionInputs = getPartitionInputs(
                        rightTable, rightInputSplits, rightKeyColumnIds, 40,
                        "pixels-lambda/" + joinedTable.getTableName() + "/" +
                                rightTable.getTableName() + "/part-");
                PartitionedTableInfo rightTableInfo = getPartitionedTableInfo(
                        rightTable, rightKeyColumnIds, rightPartitionInputs);

                List<JoinInput> joinInputs = getPartitionedJoinInputs(
                        joinedTable, parent, leftTableInfo, rightTableInfo);

                joinOperator = new PartitionedJoinOperator(
                        leftPartitionInputs, rightPartitionInputs, joinInputs, joinAlgo);
            }
            joinOperator.setChild(childOperator);
            return joinOperator;
        }
        else
        {
            throw new UnsupportedOperationException("join algorithm '" + joinAlgo +
                    "' is not supported in the joined table constructed by users");
        }
    }

    private BroadCastJoinTableInfo getBroadcastJoinTableInfo(
            Table table, List<InputSplit> inputSplits, int[] keyColumnIds)
    {
        BroadCastJoinTableInfo tableInfo = new BroadCastJoinTableInfo();
        tableInfo.setTableName(table.getTableName());
        tableInfo.setInputSplits(inputSplits);
        tableInfo.setColumnsToRead(table.getColumnNames());
        if (table.isBase())
        {
            tableInfo.setFilter(JSON.toJSONString(((BaseTable) table).getFilter()));
        }
        else
        {
            tableInfo.setFilter(JSON.toJSONString(
                    TableScanFilter.empty(table.getSchemaName(), table.getTableName())));
        }
        tableInfo.setKeyColumnIds(keyColumnIds);
        return tableInfo;
    }

    private PartitionedTableInfo getPartitionedTableInfo(
            Table table, int[] keyColumnIds, List<PartitionInput> partitionInputs)
    {
        ImmutableList.Builder<String> rightPartitionedFiles = ImmutableList.builder();
        for (PartitionInput partitionInput : partitionInputs)
        {
            rightPartitionedFiles.add(partitionInput.getOutput().getPath());
        }

        // TODO: get parallelism from config file.
        return new PartitionedTableInfo(table.getTableName(), rightPartitionedFiles.build(),
                4, table.getColumnNames(), keyColumnIds);
    }

    private List<PartitionInput> getPartitionInputs(Table inputTable, List<InputSplit> inputSplits,
                                                    int[] keyColumnIds,int numPartition, String outputBase)
    {
        List<PartitionInput> partitionInputs = new ArrayList<>();
        int outputId = 0;
        for (InputSplit inputSplit : inputSplits)
        {
            PartitionInput partitionInput = new PartitionInput();
            partitionInput.setQueryId(queryId);
            ScanTableInfo tableInfo = new ScanTableInfo();
            tableInfo.setInputSplits(ImmutableList.of(inputSplit));
            tableInfo.setColumnsToRead(inputTable.getColumnNames());
            tableInfo.setTableName(inputTable.getTableName());
            if (inputTable.isBase())
            {
                tableInfo.setFilter(JSON.toJSONString(((BaseTable) inputTable).getFilter()));
            }
            else
            {
                tableInfo.setFilter(JSON.toJSONString(
                        TableScanFilter.empty(inputTable.getSchemaName(), inputTable.getTableName())));
            }
            partitionInput.setTableInfo(tableInfo);
            partitionInput.setOutput(new OutputInfo(outputBase + outputId++, false,
                    Storage.Scheme.s3, null, null, null, true));
            partitionInput.setPartitionInfo(new PartitionInfo(keyColumnIds, numPartition));
            partitionInputs.add(partitionInput);
        }

        return partitionInputs;
    }

    private List<JoinInput> getPartitionedJoinInputs(
            JoinedTable joinedTable, Optional<JoinedTable> parent,
            PartitionedTableInfo leftTableInfo, PartitionedTableInfo rightTableInfo)
    {
        boolean postPartition = false;
        PartitionInfo postPartitionInfo = null;
        if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
        {
            postPartition = true;
            // TODO: get numPartition from the optimizer.
            postPartitionInfo = new PartitionInfo(parent.get().getJoin().getLeftKeyColumnIds(), 40);
        }

        ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
        for (int i = 0; i < 40; ++i)
        {
            // TODO: get numPartition from optimizer
            PartitionedJoinInfo joinInfo = new PartitionedJoinInfo(
                    joinedTable.getJoin().getJoinType(), joinedTable.getJoin().getLeftColumnAlias(),
                    joinedTable.getJoin().getRightColumnAlias(), joinedTable.getJoin().isIncludeKeyColumns(),
                    postPartition, postPartitionInfo, 40, ImmutableList.of(i));

            ImmutableList.Builder<String> outputFileNames = ImmutableList.builder();
            for (int j = 0; j < 4; ++j) // TODO: get parallelism from config file.
            {
                outputFileNames.add("join_" + i + "_out_" + j);
            }
            // TODO: get the path base from config file.
            MultiOutputInfo output = new MultiOutputInfo(
                    "pixels-lambda/" + joinedTable.getTableName(), Storage.Scheme.s3,
                    null, null, null, true, outputFileNames.build());

            PartitionedJoinInput joinInput = new PartitionedJoinInput(
                    queryId, leftTableInfo, rightTableInfo, joinInfo, output);

            joinInputs.add(joinInput);
        }
        return joinInputs.build();
    }

    private List<InputSplit> getInputSplits(BaseTable table) throws MetadataException, IOException
    {
        requireNonNull(table, "table is null");
        checkArgument(table.isBase(), "this is not a base table");
        ImmutableList.Builder<InputSplit> splitsBuilder = ImmutableList.builder();
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

                    for (int i = 0; i < orderedPaths.size();)
                    {
                        ImmutableList.Builder<InputInfo> inputsBuilder = ImmutableList.builder();
                        for (int j = 0; j < splitSize && j < orderedPaths.size(); ++j, ++i)
                        {
                            InputInfo input = new InputInfo(orderedPaths.get(i), 0, 1);
                            inputsBuilder.add(input);
                        }
                        splitsBuilder.add(new InputSplit(inputsBuilder.build()));
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
                            InputInfo input = new InputInfo(path, curFileRGIdx, splitSize);
                            splitsBuilder.add(new InputSplit(ImmutableList.of(input)));
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

        return splitsBuilder.build();
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
