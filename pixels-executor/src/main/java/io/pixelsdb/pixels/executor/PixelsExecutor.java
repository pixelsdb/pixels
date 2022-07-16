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
import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.metadata.domain.Projections;
import io.pixelsdb.pixels.common.metadata.domain.Splits;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.join.JoinAdvisor;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.lambda.*;
import io.pixelsdb.pixels.executor.lambda.domain.*;
import io.pixelsdb.pixels.executor.lambda.input.*;
import io.pixelsdb.pixels.executor.plan.*;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.executor.plan.Table.TableType.*;
import static java.util.Objects.requireNonNull;

/**
 * The serverless executor of join and aggregation.
 *
 * @author hank
 * @date 07/05/2022
 */
public class PixelsExecutor
{
    private static final Logger logger = LogManager.getLogger(PixelsExecutor.class);
    private static final Storage.Scheme InputStorage;
    private static final Storage.Scheme IntermediateStorage;
    private static final String IntermediateFolder;
    private static final int IntraWorkerParallelism;
    private static final int PreAggrThreshold;

    private final Table rootTable;
    private final ConfigFactory config;
    private final MetadataService metadataService;
    private final int fixedSplitSize;
    private final boolean projectionReadEnabled;
    private final boolean orderedPathEnabled;
    private final boolean compactPathEnabled;
    private final boolean computeFinalAggrInServer;
    private final Storage storage;
    private final long queryId;

    static
    {
        String storageScheme = ConfigFactory.Instance().getProperty("executor.input.storage");
        InputStorage = Storage.Scheme.from(storageScheme);
        storageScheme = ConfigFactory.Instance().getProperty("executor.intermediate.storage");
        IntermediateStorage = Storage.Scheme.from(storageScheme);
        String storageFolder = ConfigFactory.Instance().getProperty("executor.intermediate.folder");
        if (!storageFolder.endsWith("/"))
        {
            storageFolder += "/";
        }
        IntermediateFolder = storageFolder;
        IntraWorkerParallelism = Integer.parseInt(ConfigFactory.Instance()
                .getProperty("executor.intra.worker.parallelism"));
        PreAggrThreshold = Integer.parseInt(ConfigFactory.Instance()
                .getProperty("aggregation.pre-aggregate.threshold"));
    }

    /**
     * Create an executor for the join plan that is represented as a joined table.
     * In the join plan, all multi-pipeline joins are of small-left endian, all single-pipeline
     * joins have joined table on the left. There is no requirement for the end-point joins of
     * table base tables.
     *
     * @param queryId the query id
     * @param rootTable the join plan
     * @param orderedPathEnabled whether ordered path is enabled
     * @param compactPathEnabled whether compact path is enabled
     * @throws IOException
     */
    public PixelsExecutor(long queryId,
                          Table rootTable,
                          boolean orderedPathEnabled,
                          boolean compactPathEnabled) throws IOException
    {
        this.queryId = queryId;
        this.rootTable = requireNonNull(rootTable, "rootTable is null");
        checkArgument(rootTable.getTableType() == JOINED || rootTable.getTableType() == AGGREGATED,
                "currently, PixelsExecutor only supports join and aggregation");
        this.config = ConfigFactory.Instance();
        this.metadataService = new MetadataService(config.getProperty("metadata.server.host"),
                Integer.parseInt(config.getProperty("metadata.server.port")));
        this.fixedSplitSize = Integer.parseInt(config.getProperty("fixed.split.size"));
        this.projectionReadEnabled = Boolean.parseBoolean(config.getProperty("projection.read.enabled"));
        this.computeFinalAggrInServer = Boolean.parseBoolean(config.getProperty("aggregation.compute.final.in.server"));
        this.orderedPathEnabled = orderedPathEnabled;
        this.compactPathEnabled = compactPathEnabled;
        this.storage = StorageFactory.Instance().getStorage(InputStorage);
    }

    public Operator getRootOperator() throws IOException, MetadataException
    {
        if (this.rootTable.getTableType() == JOINED)
        {
            return this.getJoinOperator((JoinedTable) this.rootTable, Optional.empty());
        }
        else if (this.rootTable.getTableType() == AGGREGATED)
        {
            return this.getAggregationOperator((AggregatedTable) this.rootTable);
        }
        else
        {
            throw new UnsupportedOperationException("root table type '" +
                    this.rootTable.getTableType() + "' is currently not supported");
        }
    }

    private AggregationOperator getAggregationOperator(AggregatedTable aggregatedTable)
            throws IOException, MetadataException
    {
        requireNonNull(aggregatedTable, "aggregatedTable is null");
        Aggregation aggregation = requireNonNull(aggregatedTable.getAggregation(),
                "aggregatedTable.aggregation is null");
        Table originTable = requireNonNull(aggregation.getOriginTable(),
                "aggregation.originTable is null");
        OutputEndPoint endPoint = requireNonNull(aggregation.getOutputEndPoint(),
                "aggregation.outputEndPoint is null");

        PartialAggregationInfo partialAggregationInfo = new PartialAggregationInfo();
        partialAggregationInfo.setGroupKeyColumnAlias(aggregation.getGroupKeyColumnAlias());
        partialAggregationInfo.setResultColumnAlias(aggregation.getResultColumnAlias());
        partialAggregationInfo.setResultColumnTypes(aggregation.getResultColumnTypes());
        partialAggregationInfo.setGroupKeyColumnIds(aggregation.getGroupKeyColumnIds());
        partialAggregationInfo.setAggregateColumnIds(aggregation.getAggregateColumnIds());
        partialAggregationInfo.setFunctionTypes(aggregation.getFunctionTypes());

        String finalOutputBase = endPoint.getFolder();
        if (!finalOutputBase.endsWith("/"))
        {
            finalOutputBase += "/";
        }

        String intermediateBase = IntermediateFolder + queryId + "/" +
                aggregatedTable.getSchemaName() + "/" + aggregatedTable.getTableName() + "/";

        ImmutableList.Builder<String> partialAggrFilesBuilder = ImmutableList.builder();
        ImmutableList.Builder<ScanInput> scanInputsBuilder = ImmutableList.builder();
        JoinOperator joinOperator = null;
        boolean preAggregate;
        if (originTable.getTableType() == BASE)
        {
            List<InputSplit> inputSplits = this.getInputSplits((BaseTable) originTable);
            int outputId = 0;
            int numScanInputs = inputSplits.size() / IntraWorkerParallelism;
            if (inputSplits.size() % IntraWorkerParallelism > 0)
            {
                numScanInputs++;
            }
            preAggregate = numScanInputs > PreAggrThreshold;

            boolean[] scanProjection = new boolean[originTable.getColumnNames().length];
            Arrays.fill(scanProjection, true);

            for (int i = 0; i < inputSplits.size(); )
            {
                ScanInput scanInput = new ScanInput();
                scanInput.setQueryId(queryId);
                ScanTableInfo tableInfo = new ScanTableInfo();
                ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                        .builderWithExpectedSize(IntraWorkerParallelism);
                for (int j = 0; j < IntraWorkerParallelism && i < inputSplits.size(); ++j, ++i)
                {
                    // We assign a number of IntraWorkerParallelism input-splits to each partition worker.
                    inputsBuilder.add(inputSplits.get(i));
                }
                tableInfo.setInputSplits(inputsBuilder.build());
                tableInfo.setColumnsToRead(originTable.getColumnNames());
                tableInfo.setTableName(originTable.getTableName());
                tableInfo.setFilter(JSON.toJSONString(((BaseTable) originTable).getFilter()));
                scanInput.setTableInfo(tableInfo);
                scanInput.setScanProjection(scanProjection);
                scanInput.setPartialAggregationPresent(true);
                scanInput.setPartialAggregationInfo(partialAggregationInfo);
                String fileName = computeFinalAggrInServer && !preAggregate ? finalOutputBase : intermediateBase;
                fileName += "partial_aggr_" + outputId++;
                StorageInfo storageInfo;
                if (computeFinalAggrInServer && !preAggregate)
                {
                    storageInfo = new StorageInfo(endPoint.getScheme(), endPoint.getEndPoint(),
                            endPoint.getAccessKey(), endPoint.getSecretKey());
                }
                else
                {
                    storageInfo = new StorageInfo(IntermediateStorage, null, null, null);
                }
                scanInput.setOutput(new OutputInfo(fileName, false, storageInfo, true));
                scanInputsBuilder.add(scanInput);
                partialAggrFilesBuilder.add(fileName);
            }
        }
        else if (originTable.getTableType() == JOINED)
        {
            joinOperator = this.getJoinOperator((JoinedTable) originTable, Optional.empty());
            List<JoinInput> joinInputs = joinOperator.getJoinInputs();
            int outputId = 0;
            int numScanInputs = joinInputs.size() / IntraWorkerParallelism;
            if (joinInputs.size() % IntraWorkerParallelism > 0)
            {
                numScanInputs++;
            }
            preAggregate = numScanInputs > PreAggrThreshold;

            for (JoinInput joinInput : joinInputs)
            {
                joinInput.setPartialAggregationPresent(true);
                joinInput.setPartialAggregationInfo(partialAggregationInfo);
                String folder = computeFinalAggrInServer && !preAggregate ? finalOutputBase : intermediateBase;
                String fileName = "partial_aggr_" + outputId++;
                MultiOutputInfo outputInfo = joinInput.getOutput();
                StorageInfo storageInfo;
                if (computeFinalAggrInServer && !preAggregate)
                {
                    storageInfo = new StorageInfo(endPoint.getScheme(), endPoint.getEndPoint(),
                            endPoint.getAccessKey(), endPoint.getSecretKey());
                }
                else
                {
                    storageInfo = new StorageInfo(IntermediateStorage, null, null, null);
                }
                outputInfo.setStorageInfo(storageInfo);
                outputInfo.setPath(folder);
                outputInfo.setFileNames(ImmutableList.of(fileName));
                partialAggrFilesBuilder.add(folder + fileName);
            }
        }
        else
        {
            throw new InvalidArgumentException("origin table for aggregation must be base or joined table");
        }
        // build the pre-aggregation inputs.
        ImmutableList.Builder<AggregationInput> preAggrInputsBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> finalAggrInputFilesBuilder;
        if (preAggregate)
        {
            finalAggrInputFilesBuilder = ImmutableList.builder();
            List<String> partialAggrFiles = partialAggrFilesBuilder.build();
            boolean[] groupKeyColumnProjection = new boolean[aggregation.getGroupKeyColumnAlias().length];
            Arrays.fill(groupKeyColumnProjection, true);
            int outputId = 0;
            for (int i = 0; i < partialAggrFiles.size();)
            {
                ImmutableList.Builder<String> inputFilesBuilder = ImmutableList
                        .builderWithExpectedSize(PreAggrThreshold);
                for (int j = 0; j < PreAggrThreshold && i < partialAggrFiles.size(); ++j, ++i)
                {
                    inputFilesBuilder.add(partialAggrFiles.get(i));
                }
                AggregationInput preAggrInput = new AggregationInput();
                preAggrInput.setQueryId(queryId);
                preAggrInput.setInputFiles(inputFilesBuilder.build());
                preAggrInput.setGroupKeyColumnNames(aggregation.getGroupKeyColumnAlias());
                // Pre-aggregation should output all the group-key columns.
                preAggrInput.setGroupKeyColumnProjection(groupKeyColumnProjection);
                preAggrInput.setResultColumnNames(aggregation.getResultColumnAlias());
                preAggrInput.setResultColumnTypes(aggregation.getResultColumnTypes());
                preAggrInput.setFunctionTypes(aggregation.getFunctionTypes());
                preAggrInput.setInputStorage(new StorageInfo(IntermediateStorage,
                        null, null, null));
                StorageInfo outputStorageInfo;
                if (computeFinalAggrInServer)
                {
                    outputStorageInfo = new StorageInfo(endPoint.getScheme(), endPoint.getEndPoint(),
                            endPoint.getAccessKey(), endPoint.getSecretKey());
                }
                else
                {
                    outputStorageInfo = new StorageInfo(IntermediateStorage,
                            null, null, null);
                }
                preAggrInput.setParallelism(IntraWorkerParallelism);

                String fileName = computeFinalAggrInServer ? finalOutputBase : intermediateBase;
                fileName += "pre_aggr_" + outputId++;
                preAggrInput.setOutput(new OutputInfo(fileName, false,
                        outputStorageInfo, true));
                finalAggrInputFilesBuilder.add(fileName);
                preAggrInputsBuilder.add(preAggrInput);
            }
        }
        else
        {
            finalAggrInputFilesBuilder = partialAggrFilesBuilder;
        }
        // build the final aggregation input.
        AggregationInput finalAggrInput = new AggregationInput();
        finalAggrInput.setQueryId(queryId);
        finalAggrInput.setInputFiles(finalAggrInputFilesBuilder.build());
        finalAggrInput.setGroupKeyColumnNames(aggregation.getGroupKeyColumnAlias());
        finalAggrInput.setGroupKeyColumnProjection(aggregation.getGroupKeyColumnProjection());
        finalAggrInput.setResultColumnNames(aggregation.getResultColumnAlias());
        finalAggrInput.setResultColumnTypes(aggregation.getResultColumnTypes());
        finalAggrInput.setFunctionTypes(aggregation.getFunctionTypes());
        StorageInfo storageInfo = new StorageInfo(endPoint.getScheme(), endPoint.getEndPoint(),
                endPoint.getAccessKey(), endPoint.getSecretKey());
        if (computeFinalAggrInServer)
        {
            finalAggrInput.setInputStorage(storageInfo);
        }
        else
        {
            finalAggrInput.setInputStorage(new StorageInfo(IntermediateStorage,
                    null, null, null));
        }
        finalAggrInput.setParallelism(IntraWorkerParallelism);
        finalAggrInput.setOutput(new OutputInfo(finalOutputBase + "final_aggr",
                false, storageInfo, true));

        AggregationOperator aggregationOperator = new AggregationOperator(
                finalAggrInput, preAggrInputsBuilder.build(), scanInputsBuilder.build());
        aggregationOperator.setChild(joinOperator);

        return aggregationOperator;
    }

    private JoinOperator getMultiPipelineJoinOperator(JoinedTable joinedTable, Optional<JoinedTable> parent)
            throws IOException, MetadataException
    {
        requireNonNull(joinedTable, "joinedTable is null");
        Join join = requireNonNull(joinedTable.getJoin(), "joinedTable.join is null");
        Table leftTable = requireNonNull(join.getLeftTable(), "join.leftTable is null");
        Table rightTable = requireNonNull(join.getRightTable(), "join.rightTable is null");
        checkArgument(leftTable.getTableType() == JOINED && rightTable.getTableType() == JOINED,
                "both left and right tables should be joined tables");

        int[] leftKeyColumnIds = requireNonNull(join.getLeftKeyColumnIds(),
                "join.leftKeyColumnIds is null");
        int[] rightKeyColumnIds = requireNonNull(join.getRightKeyColumnIds(),
                "join.rightKeyColumnIds is null");
        JoinType joinType = requireNonNull(join.getJoinType(), "join.joinType is null");
        JoinEndian joinEndian = requireNonNull(join.getJoinEndian(), "join.joinEndian is null");
        checkArgument(joinEndian == JoinEndian.SMALL_LEFT,
                "multi-pipeline joins must be small-left");
        JoinAlgorithm joinAlgo = requireNonNull(join.getJoinAlgo(), "join.joinAlgo is null");

        JoinOperator leftOperator, rightOperator;
        if (joinAlgo == JoinAlgorithm.BROADCAST)
        {
            /*
            * The current join is a broadcast join.
            * The left side must be an incomplete broadcast chain join, in this case, we should either complete
            * the broadcast join if the right side is a broadcast or broadcast chain join, or build a partitioned
            * chain join if the right side is a partitioned join.
            * */
            leftOperator = getJoinOperator((JoinedTable) leftTable, Optional.of(joinedTable));
            rightOperator = getJoinOperator((JoinedTable) rightTable, Optional.empty());
            if (leftOperator.getJoinAlgo() == JoinAlgorithm.BROADCAST_CHAIN)
            {
                boolean postPartition = false;
                PartitionInfo postPartitionInfo = null;
                if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
                {
                    // Note: we must use the parent to calculate the number of partitions for post partitioning.
                    postPartition = true;
                    int numPartition = JoinAdvisor.Instance().getNumPartition(
                            parent.get().getJoin().getLeftTable(),
                            parent.get().getJoin().getRightTable(),
                            parent.get().getJoin().getJoinEndian());

                    // Check if the current table if the left child or the right child of parent.
                    if (joinedTable == parent.get().getJoin().getLeftTable())
                    {
                        postPartitionInfo = new PartitionInfo(
                                parent.get().getJoin().getLeftKeyColumnIds(), numPartition);
                    }
                    else
                    {
                        postPartitionInfo = new PartitionInfo(
                                parent.get().getJoin().getRightKeyColumnIds(), numPartition);
                    }
                }

                checkArgument(leftOperator.getJoinInputs().size() == 1,
                        "there should be exact one incomplete chain join input in the child operator");
                BroadcastChainJoinInput broadcastChainJoinInput =
                        (BroadcastChainJoinInput) leftOperator.getJoinInputs().get(0);

                if (rightOperator.getJoinAlgo() == JoinAlgorithm.BROADCAST ||
                        rightOperator.getJoinAlgo() == JoinAlgorithm.BROADCAST_CHAIN)
                {
                    List<JoinInput> rightJoinInputs = rightOperator.getJoinInputs();
                    List<InputSplit> rightInputSplits = getBroadcastInputSplits(rightJoinInputs);
                    JoinInfo joinInfo = new JoinInfo(joinType, join.getLeftColumnAlias(),
                            join.getRightColumnAlias(), join.getLeftProjection(),
                            join.getRightProjection(), postPartition, postPartitionInfo);

                    ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
                    int outputId = 0;
                    for (int i = 0; i < rightInputSplits.size(); )
                    {
                        ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                                .builderWithExpectedSize(IntraWorkerParallelism);
                        ImmutableList<String> outputs = ImmutableList.of("join_" + outputId++);
                        for (int j = 0; j < IntraWorkerParallelism && i < rightInputSplits.size(); ++j, ++i)
                        {
                            inputsBuilder.add(rightInputSplits.get(i));
                        }
                        BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                                rightTable, inputsBuilder.build(), join.getRightKeyColumnIds());

                        String path = IntermediateFolder + queryId + "/" + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/";
                        MultiOutputInfo output = new MultiOutputInfo(path,
                                new StorageInfo(IntermediateStorage, null, null, null),
                                true, outputs);

                        BroadcastChainJoinInput complete = broadcastChainJoinInput.toBuilder()
                                .setLargeTable(rightTableInfo)
                                .setJoinInfo(joinInfo)
                                .setOutput(output).build();

                        joinInputs.add(complete);
                    }

                    SingleStageJoinOperator joinOperator = new SingleStageJoinOperator(
                            joinInputs.build(), JoinAlgorithm.BROADCAST_CHAIN);
                    // The right operator must be set as the large child.
                    joinOperator.setLargeChild(rightOperator);
                    return joinOperator;
                }
                else if (rightOperator.getJoinAlgo() == JoinAlgorithm.PARTITIONED)
                {
                    /*
                     * Create a new partitioned chain join operator from the right operator,
                     * replace the join inputs with the partitioned chain join inputs,
                     * drop the right operator and return the new operator.
                     */
                    PartitionedJoinOperator rightJoinOperator = (PartitionedJoinOperator) rightOperator;
                    List<JoinInput> rightJoinInputs = rightJoinOperator.getJoinInputs();

                    List<BroadcastTableInfo> chainTables = broadcastChainJoinInput.getChainTables();
                    List<ChainJoinInfo> chainJoinInfos = broadcastChainJoinInput.getChainJoinInfos();
                    ChainJoinInfo lastChainJoinInfo = new ChainJoinInfo(joinType,
                            join.getLeftColumnAlias(), join.getRightColumnAlias(), join.getRightKeyColumnIds(),
                            join.getLeftProjection(), join.getRightProjection(), postPartition, postPartitionInfo);
                    // chainJoinInfos in broadcastChainJoin is mutable, thus we can add element directly.
                    chainJoinInfos.add(lastChainJoinInfo);

                    ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
                    for (JoinInput joinInput : rightJoinInputs)
                    {
                        PartitionedJoinInput rightJoinInput = (PartitionedJoinInput) joinInput;
                        PartitionedChainJoinInput chainJoinInput = new PartitionedChainJoinInput();
                        chainJoinInput.setQueryId(queryId);
                        chainJoinInput.setJoinInfo(rightJoinInput.getJoinInfo());
                        chainJoinInput.setOutput(rightJoinInput.getOutput());
                        chainJoinInput.setSmallTable(rightJoinInput.getSmallTable());
                        chainJoinInput.setLargeTable(rightJoinInput.getLargeTable());
                        chainJoinInput.setChainTables(chainTables);
                        chainJoinInput.setChainJoinInfos(chainJoinInfos);
                        joinInputs.add(chainJoinInput);
                    }

                    PartitionedJoinOperator joinOperator = new PartitionedJoinOperator(
                            rightJoinOperator.getSmallPartitionInputs(),
                            rightJoinOperator.getLargePartitionInputs(),
                            joinInputs.build(), JoinAlgorithm.PARTITIONED_CHAIN);
                    // Set the children of the right operator as the children of the current join operator.
                    joinOperator.setSmallChild(rightJoinOperator.getSmallChild());
                    joinOperator.setLargeChild(rightJoinOperator.getLargeChild());
                    return joinOperator;
                }
                else
                {
                    throw new InvalidArgumentException("the large-right child is a/an '" + rightOperator.getJoinAlgo() +
                            "' join which is invalid, only BROADCAST, BROADCAST_CHAIN, or PARTITIONED joins are accepted");
                }
            }
            else
            {
                throw new InvalidArgumentException("the small-left child is not a broadcast chain join whereas the " +
                        "current join is a broadcast join, such a join plan is invalid");
            }
        }
        else if (joinAlgo == JoinAlgorithm.PARTITIONED)
        {
            leftOperator = getJoinOperator((JoinedTable) leftTable, Optional.of(joinedTable));
            rightOperator = getJoinOperator((JoinedTable) rightTable, Optional.of(joinedTable));
            List<JoinInput> leftJoinInputs = leftOperator.getJoinInputs();
            List<String> leftPartitionedFiles = getPartitionedFiles(leftJoinInputs);
            List<JoinInput> rightJoinInputs = rightOperator.getJoinInputs();
            List<String> rightPartitionedFiles = getPartitionedFiles(rightJoinInputs);
            PartitionedTableInfo leftTableInfo = new PartitionedTableInfo(
                    leftTable.getTableName(), leftTable.getTableType() == BASE,
                    leftPartitionedFiles, IntraWorkerParallelism,
                    leftTable.getColumnNames(), leftKeyColumnIds);
            PartitionedTableInfo rightTableInfo = new PartitionedTableInfo(
                    rightTable.getTableName(), rightTable.getTableType() == BASE,
                    rightPartitionedFiles, IntraWorkerParallelism,
                    rightTable.getColumnNames(), rightKeyColumnIds);

            int numPartition = JoinAdvisor.Instance().getNumPartition(leftTable, rightTable, join.getJoinEndian());
            List<JoinInput> joinInputs = getPartitionedJoinInputs(
                    joinedTable, parent, numPartition, leftTableInfo, rightTableInfo);
            PartitionedJoinOperator joinOperator = new PartitionedJoinOperator(
                    null, null, joinInputs, joinAlgo);

            joinOperator.setSmallChild(leftOperator);
            joinOperator.setLargeChild(rightOperator);
            return joinOperator;
        }
        else
        {
            throw new UnsupportedOperationException("unsupported join algorithm '" + join.getJoinAlgo() +
                    "' in the joined table constructed by users");
        }
    }

    protected JoinOperator getJoinOperator(JoinedTable joinedTable, Optional<JoinedTable> parent)
            throws IOException, MetadataException
    {
        requireNonNull(joinedTable, "joinedTable is null");
        Join join = requireNonNull(joinedTable.getJoin(), "joinTable.join is null");
        requireNonNull(join.getLeftTable(), "join.leftTable is null");
        requireNonNull(join.getRightTable(), "join.rightTable is null");
        checkArgument(join.getLeftTable().getTableType() == BASE || join.getLeftTable().getTableType() == JOINED,
                "join.leftTable is not base or joined table");
        checkArgument(join.getRightTable().getTableType() == BASE || join.getRightTable().getTableType() == JOINED,
                "join.rightTable is not base or joined table");

        if (join.getLeftTable().getTableType() == JOINED && join.getRightTable().getTableType() == JOINED)
        {
            // Process multi-pipeline join.
            return getMultiPipelineJoinOperator(joinedTable, parent);
        }

        Table leftTable = join.getLeftTable();
        checkArgument(join.getRightTable().getTableType() == BASE, "join.rightTable is not base table");
        BaseTable rightTable = (BaseTable) join.getRightTable();
        int[] leftKeyColumnIds = requireNonNull(join.getLeftKeyColumnIds(),
                "join.leftKeyColumnIds is null");
        int[] rightKeyColumnIds = requireNonNull(join.getRightKeyColumnIds(),
                "join.rightKeyColumnIds is null");
        JoinType joinType = requireNonNull(join.getJoinType(), "join.joinType is null");
        JoinAlgorithm joinAlgo = requireNonNull(join.getJoinAlgo(), "join.joinAlgo is null");
        if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
        {
            checkArgument(joinAlgo != JoinAlgorithm.BROADCAST,
                    "broadcast join can not be used for LEFT_OUTER or FULL_OUTER join.");
        }

        List<InputSplit> leftInputSplits = null;
        List<String> leftPartitionedFiles = null;
        // get the rightInputSplits from metadata.
        List<InputSplit> rightInputSplits = getInputSplits(rightTable);
        JoinOperator childOperator = null;

        if (leftTable.getTableType() == BASE)
        {
            // get the leftInputSplits from metadata.
            leftInputSplits = getInputSplits((BaseTable) leftTable);
            // check if there is a chain join.
            if (joinAlgo == JoinAlgorithm.BROADCAST && parent.isPresent() &&
                    parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST &&
                    parent.get().getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                /*
                 * Chain join is found, and this is the first broadcast join in the chain.
                 * In this case, we build an incomplete chain join with only two left tables and one ChainJoinInfo.
                 */
                BroadcastTableInfo leftTableInfo = getBroadcastTableInfo(
                        leftTable, leftInputSplits, join.getLeftKeyColumnIds());
                BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                        rightTable, rightInputSplits, join.getRightKeyColumnIds());
                ChainJoinInfo chainJoinInfo;
                List<BroadcastTableInfo> chainTableInfos = new ArrayList<>();
                // deal with join endian, ensure that small table is on the left.
                if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    chainJoinInfo = new ChainJoinInfo(
                            joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                            parent.get().getJoin().getLeftKeyColumnIds(), join.getLeftProjection(),
                            join.getRightProjection(), false, null);
                    chainTableInfos.add(leftTableInfo);
                    chainTableInfos.add(rightTableInfo);
                }
                else
                {
                    chainJoinInfo = new ChainJoinInfo(
                            joinType.flip(), join.getRightColumnAlias(), join.getLeftColumnAlias(),
                            parent.get().getJoin().getLeftKeyColumnIds(), join.getRightProjection(),
                            join.getLeftProjection(), false, null);
                    chainTableInfos.add(rightTableInfo);
                    chainTableInfos.add(leftTableInfo);
                }

                BroadcastChainJoinInput broadcastChainJoinInput = new BroadcastChainJoinInput();
                broadcastChainJoinInput.setQueryId(queryId);
                broadcastChainJoinInput.setChainTables(chainTableInfos);
                List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();
                chainJoinInfos.add(chainJoinInfo);
                broadcastChainJoinInput.setChainJoinInfos(chainJoinInfos);

                return new SingleStageJoinOperator(broadcastChainJoinInput, JoinAlgorithm.BROADCAST_CHAIN);
            }
        }
        else
        {
            childOperator = getJoinOperator((JoinedTable) leftTable, Optional.of(joinedTable));
            requireNonNull(childOperator, "failed to get child operator");
            // check if there is an incomplete chain join.
            if (childOperator.getJoinAlgo() == JoinAlgorithm.BROADCAST_CHAIN &&
                    joinAlgo == JoinAlgorithm.BROADCAST &&
                    join.getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                // the current join is still a broadcast join, thus the left child is an incomplete chain join.
                if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST &&
                        parent.get().getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    /*
                     * The parent is still a small-left broadcast join, continue chain join construction by
                     * adding the right table of the current join into the left tables of the chain join
                     */
                    BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                            rightTable, rightInputSplits, join.getRightKeyColumnIds());
                    ChainJoinInfo chainJoinInfo = new ChainJoinInfo(
                            joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                            parent.get().getJoin().getLeftKeyColumnIds(), join.getLeftProjection(),
                            join.getRightProjection(), false, null);
                    checkArgument(childOperator.getJoinInputs().size() == 1,
                            "there should be exact one incomplete chain join input in the child operator");
                    BroadcastChainJoinInput broadcastChainJoinInput =
                            (BroadcastChainJoinInput) childOperator.getJoinInputs().get(0);
                    broadcastChainJoinInput.getChainTables().add(rightTableInfo);
                    broadcastChainJoinInput.getChainJoinInfos().add(chainJoinInfo);
                    // no need to create a new operator.
                    return childOperator;
                }
                else
                {
                    // The parent is not present or is not a small-left broadcast join, complete chain join construction.
                    boolean postPartition = false;
                    PartitionInfo postPartitionInfo = null;
                    if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
                    {
                        // Note: we must use the parent to calculate the number of partitions for post partitioning.
                        postPartition = true;
                        int numPartition = JoinAdvisor.Instance().getNumPartition(
                                parent.get().getJoin().getLeftTable(),
                                parent.get().getJoin().getRightTable(),
                                parent.get().getJoin().getJoinEndian());

                        /*
                        * If the program reaches here, as this is on a single-pipeline and the parent is present,
                        * the current join must be the left child of the parent. Therefore, we can use the left key
                        * column ids of the parent as the post partitioning key column ids.
                        * */
                        postPartitionInfo = new PartitionInfo(
                                parent.get().getJoin().getLeftKeyColumnIds(), numPartition);
                    }
                    JoinInfo joinInfo = new JoinInfo(joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                            join.getLeftProjection(), join.getRightProjection(), postPartition, postPartitionInfo);

                    checkArgument(childOperator.getJoinInputs().size() == 1,
                            "there should be exact one incomplete chain join input in the child operator");
                    BroadcastChainJoinInput broadcastChainJoinInput =
                            (BroadcastChainJoinInput) childOperator.getJoinInputs().get(0);

                    ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
                    int outputId = 0;
                    for (int i = 0; i < rightInputSplits.size();)
                    {
                        ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                                .builderWithExpectedSize(IntraWorkerParallelism);
                        ImmutableList<String> outputs = ImmutableList.of("join_" + outputId++);
                        for (int j = 0; j < IntraWorkerParallelism && i < rightInputSplits.size(); ++j, ++i)
                        {
                            inputsBuilder.add(rightInputSplits.get(i));
                        }
                        BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                                rightTable, inputsBuilder.build(), join.getRightKeyColumnIds());

                        String path = IntermediateFolder + queryId + "/" + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/";
                        MultiOutputInfo output = new MultiOutputInfo(path,
                                new StorageInfo(IntermediateStorage, null, null, null),
                                true, outputs);

                        BroadcastChainJoinInput complete = broadcastChainJoinInput.toBuilder()
                                .setLargeTable(rightTableInfo)
                                .setJoinInfo(joinInfo)
                                .setOutput(output).build();

                        joinInputs.add(complete);
                    }

                    return new SingleStageJoinOperator(joinInputs.build(), JoinAlgorithm.BROADCAST_CHAIN);
                }
            }
            // get the leftInputSplits or leftPartitionedFiles from childJoinInputs.
            List<JoinInput> childJoinInputs = childOperator.getJoinInputs();
            if (joinAlgo == JoinAlgorithm.BROADCAST)
            {
                leftInputSplits = getBroadcastInputSplits(childJoinInputs);
            }
            else if (joinAlgo == JoinAlgorithm.PARTITIONED)
            {
                leftPartitionedFiles = getPartitionedFiles(childJoinInputs);
            }
            else
            {
                throw new UnsupportedOperationException("unsupported join algorithm '" + joinAlgo +
                        "' in the joined table constructed by users");
            }
        }

        // generate join inputs for normal broadcast join or partitioned join.
        if (joinAlgo == JoinAlgorithm.BROADCAST)
        {
            requireNonNull(leftInputSplits, "leftInputSplits is null");

            boolean postPartition = false;
            PartitionInfo postPartitionInfo = null;
            if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
            {
                postPartition = true;
                // Note: we must use the parent to calculate the number of partitions for post partitioning.
                int numPartition = JoinAdvisor.Instance().getNumPartition(
                        parent.get().getJoin().getLeftTable(),
                        parent.get().getJoin().getRightTable(),
                        parent.get().getJoin().getJoinEndian());

                // Check if the current table if the left child or the right child of parent.
                if (joinedTable == parent.get().getJoin().getLeftTable())
                {
                    postPartitionInfo = new PartitionInfo(
                            parent.get().getJoin().getLeftKeyColumnIds(), numPartition);
                }
                else
                {
                    postPartitionInfo = new PartitionInfo(
                            parent.get().getJoin().getRightKeyColumnIds(), numPartition);
                }
            }

            int internalParallelism = IntraWorkerParallelism;
            if (leftTable.getTableType() == BASE && ((BaseTable) leftTable).getFilter().isEmpty())// &&
                    //rightTable.getFilter().isEmpty() && (leftInputSplits.size() <= 128 && rightInputSplits.size() <= 128))
            {
                /*
                 * This is used to reduce the latency of join result writing, by increasing the
                 * number of function instances (external parallelism).
                 * TODO: estimate the cost and tune the parallelism of join by histogram.
                 */
                internalParallelism = 2;
            }

            ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
            if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                BroadcastTableInfo leftTableInfo = getBroadcastTableInfo(
                        leftTable, leftInputSplits, join.getLeftKeyColumnIds());

                JoinInfo joinInfo = new JoinInfo(joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                        join.getLeftProjection(), join.getRightProjection(), postPartition, postPartitionInfo);

                int outputId = 0;
                for (int i = 0; i < rightInputSplits.size();)
                {
                    ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                            .builderWithExpectedSize(internalParallelism);
                    ImmutableList<String> outputs = ImmutableList.of("join_" + outputId++);
                    for (int j = 0; j < internalParallelism && i < rightInputSplits.size(); ++j, ++i)
                    {
                        inputsBuilder.add(rightInputSplits.get(i));
                    }
                    BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                            rightTable, inputsBuilder.build(), join.getRightKeyColumnIds());

                    String path = IntermediateFolder + queryId + "/" + joinedTable.getSchemaName() + "/" +
                            joinedTable.getTableName() + "/";
                    MultiOutputInfo output = new MultiOutputInfo(path,
                            new StorageInfo(IntermediateStorage, null, null, null),
                            true, outputs);

                    BroadcastJoinInput joinInput = new BroadcastJoinInput(
                            queryId, leftTableInfo, rightTableInfo, joinInfo,
                            false, null, output);

                    joinInputs.add(joinInput);
                }
            }
            else
            {
                BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                        rightTable, rightInputSplits, join.getRightKeyColumnIds());

                JoinInfo joinInfo = new JoinInfo(joinType.flip(), join.getRightColumnAlias(),
                        join.getLeftColumnAlias(), join.getRightProjection(), join.getLeftProjection(),
                        postPartition, postPartitionInfo);

                int outputId = 0;
                for (int i = 0; i < leftInputSplits.size();)
                {
                    ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                            .builderWithExpectedSize(internalParallelism);
                    ImmutableList<String> outputs = ImmutableList.of("join_" + outputId++);
                    for (int j = 0; j < internalParallelism && i < leftInputSplits.size(); ++j, ++i)
                    {
                        inputsBuilder.add(leftInputSplits.get(i));
                    }
                    BroadcastTableInfo leftTableInfo = getBroadcastTableInfo(
                            leftTable, inputsBuilder.build(), join.getLeftKeyColumnIds());

                    String path = IntermediateFolder + queryId + "/" + joinedTable.getSchemaName() + "/" +
                            joinedTable.getTableName() + "/";
                    MultiOutputInfo output = new MultiOutputInfo(path,
                            new StorageInfo(IntermediateStorage, null, null, null),
                            true, outputs);

                    BroadcastJoinInput joinInput = new BroadcastJoinInput(
                            queryId, rightTableInfo, leftTableInfo, joinInfo,
                            false, null, output);

                    joinInputs.add(joinInput);
                }
            }
            SingleStageJoinOperator joinOperator =
                    new SingleStageJoinOperator(joinInputs.build(), joinAlgo);
            if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                joinOperator.setSmallChild(childOperator);
            }
            else
            {
                joinOperator.setLargeChild(childOperator);
            }
            return joinOperator;
        }
        else if (joinAlgo == JoinAlgorithm.PARTITIONED)
        {
            // process partitioned join.
            PartitionedJoinOperator joinOperator;
            int numPartition = JoinAdvisor.Instance().getNumPartition(leftTable, rightTable, join.getJoinEndian());
            if (childOperator != null)
            {
                // left side is post partitioned, thus we only partition the right table.
                PartitionedTableInfo leftTableInfo = new PartitionedTableInfo(
                        leftTable.getTableName(), leftTable.getTableType() == BASE,
                        leftPartitionedFiles, IntraWorkerParallelism,
                        leftTable.getColumnNames(), leftKeyColumnIds);

                List<PartitionInput> rightPartitionInputs = getPartitionInputs(
                        rightTable, rightInputSplits, rightKeyColumnIds, numPartition,
                        IntermediateFolder + queryId + "/" + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/" + rightTable.getTableName() + "/part-");

                PartitionedTableInfo rightTableInfo = getPartitionedTableInfo(
                        rightTable, rightKeyColumnIds, rightPartitionInputs);

                List<JoinInput> joinInputs = getPartitionedJoinInputs(
                        joinedTable, parent, numPartition, leftTableInfo, rightTableInfo);

                if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    joinOperator = new PartitionedJoinOperator(
                            null, rightPartitionInputs, joinInputs, joinAlgo);
                    joinOperator.setSmallChild(childOperator);
                }
                else
                {
                    joinOperator = new PartitionedJoinOperator(
                            rightPartitionInputs, null, joinInputs, joinAlgo);
                    joinOperator.setLargeChild(childOperator);
                }
            }
            else
            {
                // partition both tables. in this case, the operator's child must be null.
                List<PartitionInput> leftPartitionInputs = getPartitionInputs(
                        leftTable, leftInputSplits, leftKeyColumnIds, numPartition,
                        IntermediateFolder + queryId + "/" + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/" + leftTable.getTableName() + "/part-");
                PartitionedTableInfo leftTableInfo = getPartitionedTableInfo(
                        leftTable, leftKeyColumnIds, leftPartitionInputs);

                List<PartitionInput> rightPartitionInputs = getPartitionInputs(
                        rightTable, rightInputSplits, rightKeyColumnIds, numPartition,
                        IntermediateFolder + queryId + "/" + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/" + rightTable.getTableName() + "/part-");
                PartitionedTableInfo rightTableInfo = getPartitionedTableInfo(
                        rightTable, rightKeyColumnIds, rightPartitionInputs);

                List<JoinInput> joinInputs = getPartitionedJoinInputs(
                        joinedTable, parent, numPartition, leftTableInfo, rightTableInfo);

                if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    joinOperator = new PartitionedJoinOperator(
                            leftPartitionInputs, rightPartitionInputs, joinInputs, joinAlgo);
                }
                else
                {
                    joinOperator = new PartitionedJoinOperator(
                            rightPartitionInputs, leftPartitionInputs, joinInputs, joinAlgo);
                }
            }
            return joinOperator;
        }
        else
        {
            throw new UnsupportedOperationException("unsupported join algorithm '" + joinAlgo +
                    "' in the joined table constructed by users");
        }
    }

    /**
     * Get the partitioned file paths from the join inputs of the child.
     * @param joinInputs the join inputs of the child
     * @return the paths of the partitioned files
     */
    private List<String> getPartitionedFiles(List<JoinInput> joinInputs)
    {
        ImmutableList.Builder<String> partitionedFiles = ImmutableList.builder();
        for (JoinInput joinInput : joinInputs)
        {
            MultiOutputInfo output = joinInput.getOutput();
            String base = output.getPath();
            if (!base.endsWith("/"))
            {
                base += "/";
            }
            for (String fileName : output.getFileNames())
            {
                partitionedFiles.add(base + fileName);
            }
        }
        return partitionedFiles.build();
    }

    /**
     * Get the input splits for a broadcast join from the join inputs of the child.
     * @param joinInputs the join inputs of the child
     * @return the input splits for the broadcast join
     */
    private List<InputSplit> getBroadcastInputSplits(List<JoinInput> joinInputs)
    {
        ImmutableList.Builder<InputSplit> inputSplits = ImmutableList.builder();
        for (JoinInput joinInput : joinInputs)
        {
            MultiOutputInfo output = joinInput.getOutput();
            String base = output.getPath();
            if (!base.endsWith("/"))
            {
                base += "/";
            }
            /*
            * We make each input file as an input split, thus the number of broadcast join workers
            * will be the same as the number of workers of the child join. This provides better
            * parallelism and is required by partitioned chain join.
            * */
            for (String fileName : output.getFileNames())
            {
                InputInfo inputInfo = new InputInfo(base + fileName, 0, -1);
                inputSplits.add(new InputSplit(ImmutableList.of(inputInfo)));
            }
        }
        return inputSplits.build();
    }

    private BroadcastTableInfo getBroadcastTableInfo(
            Table table, List<InputSplit> inputSplits, int[] keyColumnIds)
    {
        BroadcastTableInfo tableInfo = new BroadcastTableInfo();
        tableInfo.setTableName(table.getTableName());
        tableInfo.setBase(table.getTableType() == BASE);
        tableInfo.setInputSplits(inputSplits);
        tableInfo.setColumnsToRead(table.getColumnNames());
        if (table.getTableType() == BASE)
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

        return new PartitionedTableInfo(table.getTableName(), table.getTableType() == BASE,
                rightPartitionedFiles.build(), IntraWorkerParallelism, table.getColumnNames(), keyColumnIds);
    }

    private List<PartitionInput> getPartitionInputs(Table inputTable, List<InputSplit> inputSplits,
                                                    int[] keyColumnIds, int numPartition, String outputBase)
    {
        ImmutableList.Builder<PartitionInput> partitionInputsBuilder = ImmutableList.builder();
        int outputId = 0;
        for (int i = 0; i < inputSplits.size();)
        {
            PartitionInput partitionInput = new PartitionInput();
            partitionInput.setQueryId(queryId);
            ScanTableInfo tableInfo = new ScanTableInfo();
            ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                    .builderWithExpectedSize(IntraWorkerParallelism);
            for (int j = 0; j < IntraWorkerParallelism && i < inputSplits.size(); ++j, ++i)
            {
                // We assign a number of IntraWorkerParallelism input-splits to each partition worker.
                inputsBuilder.add(inputSplits.get(i));
            }
            tableInfo.setInputSplits(inputsBuilder.build());
            tableInfo.setColumnsToRead(inputTable.getColumnNames());
            tableInfo.setTableName(inputTable.getTableName());
            if (inputTable.getTableType() == BASE)
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
                    new StorageInfo(InputStorage, null, null, null), true));
            partitionInput.setPartitionInfo(new PartitionInfo(keyColumnIds, numPartition));
            partitionInputsBuilder.add(partitionInput);
        }

        return partitionInputsBuilder.build();
    }

    /**
     * Get the join inputs of a partitioned join, given the left and right children.
     * @param joinedTable this joined table
     * @param parent the parent of this joined table
     * @param numPartition the number of partitions
     * @param leftTableInfo the left table
     * @param rightTableInfo the right table
     * @return the join input of this join
     * @throws MetadataException
     */
    private List<JoinInput> getPartitionedJoinInputs(
            JoinedTable joinedTable, Optional<JoinedTable> parent, int numPartition,
            PartitionedTableInfo leftTableInfo, PartitionedTableInfo rightTableInfo) throws MetadataException
    {
        boolean postPartition = false;
        PartitionInfo postPartitionInfo = null;
        if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
        {
            postPartition = true;
            // Note: DO NOT use numPartition as the number of partitions for post partitioning.
            int numPostPartition = JoinAdvisor.Instance().getNumPartition(
                    parent.get().getJoin().getLeftTable(),
                    parent.get().getJoin().getRightTable(),
                    parent.get().getJoin().getJoinEndian());

            // Check if the current table if the left child or the right child of parent.
            if (joinedTable == parent.get().getJoin().getLeftTable())
            {
                postPartitionInfo = new PartitionInfo(parent.get().getJoin().getLeftKeyColumnIds(), numPostPartition);
            }
            else
            {
                postPartitionInfo = new PartitionInfo(parent.get().getJoin().getRightKeyColumnIds(), numPostPartition);
            }
        }

        ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
        for (int i = 0; i < numPartition; ++i)
        {
            ImmutableList.Builder<String> outputFileNames = ImmutableList
                    .builderWithExpectedSize(IntraWorkerParallelism);
            outputFileNames.add("join_" + i);
            if (joinedTable.getJoin().getJoinType() == JoinType.EQUI_LEFT ||
                    joinedTable.getJoin().getJoinType() == JoinType.EQUI_FULL)
            {
                outputFileNames.add("join_" + i + "_left");
            }

            String path = IntermediateFolder + queryId + "/" + joinedTable.getSchemaName() + "/" +
                    joinedTable.getTableName() + "/";
            MultiOutputInfo output = new MultiOutputInfo(path,
                    new StorageInfo(IntermediateStorage, null, null, null),
                    true, outputFileNames.build());

            PartitionedJoinInput joinInput;
            if (joinedTable.getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo(joinedTable.getJoin().getJoinType(),
                        joinedTable.getJoin().getLeftColumnAlias(), joinedTable.getJoin().getRightColumnAlias(),
                        joinedTable.getJoin().getLeftProjection(), joinedTable.getJoin().getRightProjection(),
                        postPartition, postPartitionInfo, numPartition, ImmutableList.of(i));
                 joinInput = new PartitionedJoinInput(queryId, leftTableInfo, rightTableInfo, joinInfo,
                         false, null, output);
            }
            else
            {
                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo(joinedTable.getJoin().getJoinType().flip(),
                        joinedTable.getJoin().getRightColumnAlias(), joinedTable.getJoin().getLeftColumnAlias(),
                        joinedTable.getJoin().getRightProjection(), joinedTable.getJoin().getLeftProjection(),
                        postPartition, postPartitionInfo, numPartition, ImmutableList.of(i));
                joinInput = new PartitionedJoinInput(queryId, rightTableInfo, leftTableInfo, joinInfo,
                        false, null, output);
            }

            joinInputs.add(joinInput);
        }
        return joinInputs.build();
    }

    private List<InputSplit> getInputSplits(BaseTable table) throws MetadataException, IOException
    {
        requireNonNull(table, "table is null");
        checkArgument(table.getTableType() == BASE, "this is not a base table");
        ImmutableList.Builder<InputSplit> splitsBuilder = ImmutableList.builder();
        int splitSize = 0;
        List<Layout> layouts = metadataService.getLayouts(table.getSchemaName(), table.getTableName());
        for (Layout layout : layouts)
        {
            int version = layout.getVersion();
            SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
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
            } else
            {
                // log.info("columns to be accessed: " + columnSet.toString());
                SplitsIndex splitsIndex = IndexFactory.Instance().getSplitsIndex(schemaTableName);
                if (splitsIndex == null)
                {
                    logger.debug("splits index not exist in factory, building index...");
                    splitsIndex = buildSplitsIndex(order, splits, schemaTableName);
                } else
                {
                    int indexVersion = splitsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("splits index version is not up-to-date, updating index...");
                        splitsIndex = buildSplitsIndex(order, splits, schemaTableName);
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
                ProjectionsIndex projectionsIndex = IndexFactory.Instance().getProjectionsIndex(schemaTableName);
                Projections projections = JSON.parseObject(layout.getProjections(), Projections.class);
                if (projectionsIndex == null)
                {
                    logger.debug("projections index not exist in factory, building index...");
                    projectionsIndex = buildProjectionsIndex(order, projections, schemaTableName);
                }
                else
                {
                    int indexVersion = projectionsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("projections index is not up-to-date, updating index...");
                        projectionsIndex = buildProjectionsIndex(order, projections, schemaTableName);
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
                        ImmutableList.Builder<InputInfo> inputsBuilder =
                                ImmutableList.builderWithExpectedSize(splitSize);
                        for (int j = 0; j < splitSize && i < orderedPaths.size(); ++j, ++i)
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

    private SplitsIndex buildSplitsIndex(Order order, Splits splits, SchemaTableName schemaTableName)
            throws MetadataException
    {
        List<String> columnOrder = order.getColumnOrder();
        SplitsIndex index;
        String indexTypeName = ConfigFactory.Instance().getProperty("splits.index.type");
        SplitsIndex.IndexType indexType = SplitsIndex.IndexType.valueOf(indexTypeName.toUpperCase());
        switch (indexType)
        {
            case INVERTED:
                index = new InvertedSplitsIndex(columnOrder, SplitPattern.buildPatterns(columnOrder, splits),
                        splits.getNumRowGroupInBlock());
                break;
            case COST_BASED:
                index = new CostBasedSplitsIndex(this.metadataService, schemaTableName,
                        splits.getNumRowGroupInBlock(), splits.getNumRowGroupInBlock());
                break;
            default:
                throw new UnsupportedOperationException("splits index type '" + indexType + "' is not supported");
        }

        IndexFactory.Instance().cacheSplitsIndex(schemaTableName, index);
        return index;
    }

    private ProjectionsIndex buildProjectionsIndex(Order order, Projections projections, SchemaTableName schemaTableName) {
        List<String> columnOrder = order.getColumnOrder();
        ProjectionsIndex index;
        index = new InvertedProjectionsIndex(columnOrder, ProjectionPattern.buildPatterns(columnOrder, projections));
        IndexFactory.Instance().cacheProjectionsIndex(schemaTableName, index);
        return index;
    }
}
