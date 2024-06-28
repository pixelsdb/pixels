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
package io.pixelsdb.pixels.planner;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.PlanOptimizer;
import io.pixelsdb.pixels.planner.plan.logical.*;
import io.pixelsdb.pixels.planner.plan.logical.Table;
import io.pixelsdb.pixels.planner.plan.physical.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The serverless executor of join and aggregation.
 *
 * @author hank
 * @create 2022-05-07
 */
public class PixelsPlanner
{
    private static final Logger logger = LogManager.getLogger(PixelsPlanner.class);
    private static final StorageInfo InputStorageInfo;
    private static final StorageInfo IntermediateStorageInfo;
    private static final String IntermediateFolder;
    private static final int IntraWorkerParallelism;
    private static final ExchangeMethod EnabledExchangeMethod;

    private final Table rootTable;
    private final ConfigFactory config;
    private final MetadataService metadataService;
    private final int fixedSplitSize;
    private final boolean projectionReadEnabled;
    private final boolean orderedPathEnabled;
    private final boolean compactPathEnabled;
    private final Storage storage;
    private final long transId;

    /**
     * The data size in bytes to be scanned by the input query.
     */
    private double scanSize = 0;

    static
    {
        Storage.Scheme inputStorageScheme = Storage.Scheme.from(
                ConfigFactory.Instance().getProperty("executor.input.storage.scheme"));
        InputStorageInfo = StorageInfoBuilder.BuildFromConfig(inputStorageScheme);

        Storage.Scheme interStorageScheme = Storage.Scheme.from(
                ConfigFactory.Instance().getProperty("executor.intermediate.storage.scheme"));
        IntermediateStorageInfo = StorageInfoBuilder.BuildFromConfig(interStorageScheme);
        String interStorageFolder = ConfigFactory.Instance().getProperty("executor.intermediate.folder");
        if (!interStorageFolder.endsWith("/"))
        {
            interStorageFolder += "/";
        }
        IntermediateFolder = interStorageFolder;
        IntraWorkerParallelism = Integer.parseInt(ConfigFactory.Instance()
                .getProperty("executor.intra.worker.parallelism"));
        EnabledExchangeMethod = ExchangeMethod.from(
                ConfigFactory.Instance().getProperty("executor.exchange.method"));
    }

    /**
     * Create an executor for the join plan that is represented as a joined table.
     * In the join plan, all multi-pipeline joins are of small-left endian, all single-pipeline
     * joins have joined table on the left. There is no requirement for the end-point joins of
     * table base tables.
     *
     * @param transId the transaction id
     * @param rootTable the join plan
     * @param orderedPathEnabled whether ordered path is enabled
     * @param compactPathEnabled whether compact path is enabled
     * @param metadataService the metadata service to access Pixels metadata
     * @throws IOException
     */
    public PixelsPlanner(long transId, Table rootTable, boolean orderedPathEnabled, boolean compactPathEnabled,
                         Optional<MetadataService> metadataService) throws IOException
    {
        this.transId = transId;
        this.rootTable = requireNonNull(rootTable, "rootTable is null");
        checkArgument(rootTable.getTableType() == Table.TableType.BASE ||
                        rootTable.getTableType() == Table.TableType.JOINED ||
                        rootTable.getTableType() == Table.TableType.AGGREGATED,
                "currently, PixelsPlanner only supports scan, join, and aggregation");
        this.config = ConfigFactory.Instance();
        this.metadataService = metadataService.orElseGet(() ->
                new MetadataService(config.getProperty("metadata.server.host"),
                        Integer.parseInt(config.getProperty("metadata.server.port"))));
        this.fixedSplitSize = Integer.parseInt(config.getProperty("fixed.split.size"));
        this.projectionReadEnabled = Boolean.parseBoolean(config.getProperty("projection.read.enabled"));
        this.orderedPathEnabled = orderedPathEnabled;
        this.compactPathEnabled = compactPathEnabled;
        this.storage = StorageFactory.Instance().getStorage(InputStorageInfo.getScheme());
    }

    public Operator getRootOperator() throws IOException, MetadataException
    {
        if (this.rootTable.getTableType() == Table.TableType.BASE)
        {
            return this.getScanOperator((BaseTable) this.rootTable);
        }
        else if (this.rootTable.getTableType() == Table.TableType.JOINED)
        {
            return this.getJoinOperator((JoinedTable) this.rootTable, Optional.empty());
        }
        else if (this.rootTable.getTableType() == Table.TableType.AGGREGATED)
        {
            return this.getAggregationOperator((AggregatedTable) this.rootTable);
        }
        else
        {
            throw new UnsupportedOperationException("root table type '" +
                    this.rootTable.getTableType() + "' is currently not supported");
        }
    }

    public double getScanSize()
    {
        return scanSize;
    }

    public static String getIntermediateFolderForTrans(long transId)
    {
        return IntermediateFolder + transId + "/";
    }

    private ScanOperator getScanOperator(BaseTable scanTable) throws IOException, MetadataException
    {
        final String intermediateBase = getIntermediateFolderForTrans(transId) +
                scanTable.getSchemaName() + "/" + scanTable.getTableName() + "/";
        ImmutableList.Builder<ScanInput> scanInputsBuilder = ImmutableList.builder();
        List<InputSplit> inputSplits = this.getInputSplits(scanTable);
        int outputId = 0;
        boolean[] scanProjection = new boolean[scanTable.getColumnNames().length];
        Arrays.fill(scanProjection, true);

        for (int i = 0; i < inputSplits.size(); )
        {
            ScanInput scanInput = new ScanInput();
            scanInput.setTransId(transId);
            ScanTableInfo tableInfo = new ScanTableInfo();
            ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                    .builderWithExpectedSize(IntraWorkerParallelism);
            for (int j = 0; j < IntraWorkerParallelism && i < inputSplits.size(); ++j, ++i)
            {
                // We assign a number of IntraWorkerParallelism input-splits to each partition worker.
                inputsBuilder.add(inputSplits.get(i));
            }
            tableInfo.setInputSplits(inputsBuilder.build());
            tableInfo.setColumnsToRead(scanTable.getColumnNames());
            tableInfo.setTableName(scanTable.getTableName());
            tableInfo.setFilter(JSON.toJSONString(scanTable.getFilter()));
            tableInfo.setBase(true);
            tableInfo.setStorageInfo(InputStorageInfo);
            scanInput.setTableInfo(tableInfo);
            scanInput.setScanProjection(scanProjection);
            scanInput.setPartialAggregationPresent(false);
            scanInput.setPartialAggregationInfo(null);
            String folderName = intermediateBase + (outputId++) + "/";
            scanInput.setOutput(new OutputInfo(folderName, IntermediateStorageInfo, true));
            scanInputsBuilder.add(scanInput);
        }
        return EnabledExchangeMethod == ExchangeMethod.batch ?
                new ScanBatchOperator(scanTable.getTableName(), scanInputsBuilder.build()) :
                new ScanStreamOperator(scanTable.getTableName(), scanInputsBuilder.build());
    }

    private AggregationOperator getAggregationOperator(AggregatedTable aggregatedTable)
            throws IOException, MetadataException
    {
        requireNonNull(aggregatedTable, "aggregatedTable is null");
        Aggregation aggregation = requireNonNull(aggregatedTable.getAggregation(),
                "aggregatedTable.aggregation is null");
        Table originTable = requireNonNull(aggregation.getOriginTable(),
                "aggregation.originTable is null");

        PartialAggregationInfo partialAggregationInfo = new PartialAggregationInfo();
        partialAggregationInfo.setGroupKeyColumnAlias(aggregation.getGroupKeyColumnAlias());
        partialAggregationInfo.setResultColumnAlias(aggregation.getResultColumnAlias());
        partialAggregationInfo.setResultColumnTypes(aggregation.getResultColumnTypes());
        partialAggregationInfo.setGroupKeyColumnIds(aggregation.getGroupKeyColumnIds());
        partialAggregationInfo.setAggregateColumnIds(aggregation.getAggregateColumnIds());
        partialAggregationInfo.setFunctionTypes(aggregation.getFunctionTypes());
        int numPartitions = PlanOptimizer.Instance().getAggrNumPartitions(this.transId, aggregatedTable);
        partialAggregationInfo.setPartition(numPartitions > 1);
        partialAggregationInfo.setNumPartition(numPartitions);

        final String intermediateBase = getIntermediateFolderForTrans(transId) +
                aggregatedTable.getSchemaName() + "/" + aggregatedTable.getTableName() + "/";

        ImmutableList.Builder<String> aggrInputFilesBuilder = ImmutableList.builder();
        ImmutableList.Builder<ScanInput> scanInputsBuilder = ImmutableList.builder();
        JoinOperator joinOperator = null;
        if (originTable.getTableType() == Table.TableType.BASE)
        {
            List<InputSplit> inputSplits = this.getInputSplits((BaseTable) originTable);
            int outputId = 0;

            boolean[] scanProjection = new boolean[originTable.getColumnNames().length];
            Arrays.fill(scanProjection, true);

            for (int i = 0; i < inputSplits.size(); )
            {
                ScanInput scanInput = new ScanInput();
                scanInput.setTransId(transId);
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
                tableInfo.setBase(true);
                tableInfo.setStorageInfo(InputStorageInfo);
                scanInput.setTableInfo(tableInfo);
                scanInput.setScanProjection(scanProjection);
                scanInput.setPartialAggregationPresent(true);
                scanInput.setPartialAggregationInfo(partialAggregationInfo);
                String fileName = intermediateBase + (outputId++) + "/partial_aggr";
                scanInput.setOutput(new OutputInfo(fileName, IntermediateStorageInfo, true));
                scanInputsBuilder.add(scanInput);
                aggrInputFilesBuilder.add(fileName);
            }
        }
        else if (originTable.getTableType() == Table.TableType.JOINED)
        {
            joinOperator = this.getJoinOperator((JoinedTable) originTable, Optional.empty());
            List<JoinInput> joinInputs = joinOperator.getJoinInputs();
            int outputId = 0;

            for (JoinInput joinInput : joinInputs)
            {
                joinInput.setPartialAggregationPresent(true);
                joinInput.setPartialAggregationInfo(partialAggregationInfo);
                String fileName = "partial_aggr_" + outputId++;
                MultiOutputInfo outputInfo = joinInput.getOutput();
                outputInfo.setStorageInfo(IntermediateStorageInfo);
                outputInfo.setPath(intermediateBase);
                outputInfo.setFileNames(ImmutableList.of(fileName));
                aggrInputFilesBuilder.add(intermediateBase + fileName);
            }
        }
        else
        {
            throw new InvalidArgumentException("origin table for aggregation must be base or joined table");
        }
        // build the final-aggregation inputs.
        List<String> aggrInputFiles = aggrInputFilesBuilder.build();
        ImmutableList.Builder<AggregationInput> finalAggrInputsBuilder = ImmutableList.builder();
        int columnId = 0;
        int[] groupKeyColumnIds = new int[aggregation.getGroupKeyColumnAlias().length];
        for (int i = 0; i < groupKeyColumnIds.length; ++i)
        {
            groupKeyColumnIds[i] = columnId++;
        }
        int[] aggrColumnIds = new int[aggregation.getResultColumnAlias().length];
        for (int i = 0; i < aggrColumnIds.length; ++i)
        {
            aggrColumnIds[i] = columnId++;
        }
        String[] columnsToRead = ObjectArrays.concat(
                aggregation.getGroupKeyColumnAlias(), aggregation.getResultColumnAlias(), String.class);
        for (int hash = 0; hash < numPartitions; ++hash)
        {
            AggregationInput finalAggrInput = new AggregationInput();
            finalAggrInput.setTransId(transId);
            AggregatedTableInfo aggregatedTableInfo = new AggregatedTableInfo();
            aggregatedTableInfo.setTableName(aggregatedTable.getTableName());
            aggregatedTableInfo.setBase(false);
            aggregatedTableInfo.setInputFiles(aggrInputFiles);
            aggregatedTableInfo.setColumnsToRead(columnsToRead);
            aggregatedTableInfo.setStorageInfo(IntermediateStorageInfo);
            aggregatedTableInfo.setParallelism(IntraWorkerParallelism);
            finalAggrInput.setAggregatedTableInfo(aggregatedTableInfo);
            AggregationInfo aggregationInfo = new AggregationInfo();
            aggregationInfo.setInputPartitioned(numPartitions > 1);
            aggregationInfo.setHashValues(ImmutableList.of(hash));
            aggregationInfo.setNumPartition(numPartitions);
            aggregationInfo.setGroupKeyColumnIds(groupKeyColumnIds);
            aggregationInfo.setAggregateColumnIds(aggrColumnIds);
            aggregationInfo.setGroupKeyColumnNames(aggregation.getGroupKeyColumnAlias());
            aggregationInfo.setGroupKeyColumnProjection(aggregation.getGroupKeyColumnProjection());
            aggregationInfo.setResultColumnNames(aggregation.getResultColumnAlias());
            aggregationInfo.setResultColumnTypes(aggregation.getResultColumnTypes());
            aggregationInfo.setFunctionTypes(aggregation.getFunctionTypes());
            finalAggrInput.setAggregationInfo(aggregationInfo);
            String fileName = intermediateBase + (hash) + "/final_aggr";
            finalAggrInput.setOutput(new OutputInfo(fileName, IntermediateStorageInfo, true));
            finalAggrInputsBuilder.add(finalAggrInput);
        }

        AggregationOperator aggregationOperator = EnabledExchangeMethod == ExchangeMethod.batch ?
                new AggregationBatchOperator(aggregatedTable.getTableName(),
                        finalAggrInputsBuilder.build(), scanInputsBuilder.build()) :
                new AggregationStreamOperator(aggregatedTable.getTableName(),
                        finalAggrInputsBuilder.build(), scanInputsBuilder.build());
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
        checkArgument(leftTable.getTableType() == Table.TableType.JOINED && rightTable.getTableType() == Table.TableType.JOINED,
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
                // Issue #487: add incomplete check.
                checkArgument(!leftOperator.isComplete(), "left broadcast chain join should be incomplete");
                boolean postPartition = false;
                PartitionInfo postPartitionInfo = null;
                if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
                {
                    // Note: we must use the parent to calculate the number of partitions for post partitioning.
                    postPartition = true;
                    int numPartition = PlanOptimizer.Instance().getJoinNumPartition(
                            this.transId,
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
                    for (InputSplit rightInputSplit : rightInputSplits)
                    {
                        ImmutableList<String> outputs = ImmutableList.of((outputId++) + "/join");
                        BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                                rightTable, ImmutableList.of(rightInputSplit), join.getRightKeyColumnIds());

                        String path = getIntermediateFolderForTrans(transId) + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/";
                        MultiOutputInfo output = new MultiOutputInfo(path, IntermediateStorageInfo, true, outputs);

                        BroadcastChainJoinInput complete = broadcastChainJoinInput.toBuilder()
                                .setLargeTable(rightTableInfo)
                                .setJoinInfo(joinInfo)
                                .setOutput(output).build();

                        joinInputs.add(complete);
                    }

                    SingleStageJoinOperator joinOperator = EnabledExchangeMethod == ExchangeMethod.batch ?
                            new SingleStageJoinBatchOperator(joinedTable.getTableName(), true,
                                    joinInputs.build(), JoinAlgorithm.BROADCAST_CHAIN) :
                            new SingleStageJoinStreamOperator(joinedTable.getTableName(), true,
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
                        chainJoinInput.setTransId(transId);
                        chainJoinInput.setJoinInfo(rightJoinInput.getJoinInfo());
                        chainJoinInput.setOutput(rightJoinInput.getOutput());
                        chainJoinInput.setSmallTable(rightJoinInput.getSmallTable());
                        chainJoinInput.setLargeTable(rightJoinInput.getLargeTable());
                        chainJoinInput.setChainTables(chainTables);
                        chainJoinInput.setChainJoinInfos(chainJoinInfos);
                        joinInputs.add(chainJoinInput);
                    }

                    PartitionedJoinOperator joinOperator = EnabledExchangeMethod == ExchangeMethod.batch ?
                            new PartitionedJoinBatchOperator(joinedTable.getTableName(),
                                    rightJoinOperator.getSmallPartitionInputs(),
                                    rightJoinOperator.getLargePartitionInputs(),
                                    joinInputs.build(), JoinAlgorithm.PARTITIONED_CHAIN) :
                            new PartitionedJoinStreamOperator(joinedTable.getTableName(),
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
                // FIXME: this is possible, the small-left broadcast join might not be chained successfully
                //  (e.g., TPC-H q8 with all joins using broadcast)
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
            boolean leftIsBase = leftTable.getTableType() == Table.TableType.BASE;
            PartitionedTableInfo leftTableInfo = new PartitionedTableInfo(
                    leftTable.getTableName(), leftIsBase, leftTable.getColumnNames(),
                    leftIsBase ? InputStorageInfo : IntermediateStorageInfo,
                    leftPartitionedFiles, IntraWorkerParallelism, leftKeyColumnIds);
            boolean rightIsBase = rightTable.getTableType() == Table.TableType.BASE;
            PartitionedTableInfo rightTableInfo = new PartitionedTableInfo(
                    rightTable.getTableName(), rightIsBase, rightTable.getColumnNames(),
                    rightIsBase ? InputStorageInfo : IntermediateStorageInfo,
                    rightPartitionedFiles, IntraWorkerParallelism, rightKeyColumnIds);

            int numPartition = PlanOptimizer.Instance().getJoinNumPartition(
                    this.transId, leftTable, rightTable, join.getJoinEndian());
            List<JoinInput> joinInputs = getPartitionedJoinInputs(
                    joinedTable, parent, numPartition, leftTableInfo, rightTableInfo,
                    null, null);
            PartitionedJoinOperator joinOperator = EnabledExchangeMethod == ExchangeMethod.batch ?
                    new PartitionedJoinBatchOperator(joinedTable.getTableName(),
                            null, null, joinInputs, joinAlgo) :
                    new PartitionedJoinStreamOperator(joinedTable.getTableName(),
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

    /**
     * Get the join operator of a single left-deep pipeline of joins. All the nodes (joins) in this pipeline
     * have a base table on its right child. Thus, if one node is a join, then it must be the left child of
     * it parent.
     *
     * @param joinedTable the root of the pipeline of joins
     * @param parent the parent, if present, of this pipeline
     * @return the root join operator for this pipeline
     * @throws IOException
     * @throws MetadataException
     */
    private JoinOperator getJoinOperator(JoinedTable joinedTable, Optional<JoinedTable> parent)
            throws IOException, MetadataException
    {
        requireNonNull(joinedTable, "joinedTable is null");
        Join join = requireNonNull(joinedTable.getJoin(), "joinTable.join is null");
        requireNonNull(join.getLeftTable(), "join.leftTable is null");
        requireNonNull(join.getRightTable(), "join.rightTable is null");
        checkArgument(join.getLeftTable().getTableType() == Table.TableType.BASE ||
                        join.getLeftTable().getTableType() == Table.TableType.JOINED,
                "join.leftTable is not base or joined table");
        checkArgument(join.getRightTable().getTableType() == Table.TableType.BASE ||
                        join.getRightTable().getTableType() == Table.TableType.JOINED,
                "join.rightTable is not base or joined table");

        if (join.getLeftTable().getTableType() == Table.TableType.JOINED &&
                join.getRightTable().getTableType() == Table.TableType.JOINED)
        {
            // Process multi-pipeline join.
            return getMultiPipelineJoinOperator(joinedTable, parent);
        }

        Table leftTable = join.getLeftTable();
        checkArgument(join.getRightTable().getTableType() == Table.TableType.BASE,
                "join.rightTable is not base table");
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

        if (leftTable.getTableType() == Table.TableType.BASE)
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
                broadcastChainJoinInput.setTransId(transId);
                broadcastChainJoinInput.setChainTables(chainTableInfos);
                List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();
                chainJoinInfos.add(chainJoinInfo);
                broadcastChainJoinInput.setChainJoinInfos(chainJoinInfos);

                return EnabledExchangeMethod == ExchangeMethod.batch ?
                        new SingleStageJoinBatchOperator(joinedTable.getTableName(), false,
                                broadcastChainJoinInput, JoinAlgorithm.BROADCAST_CHAIN) :
                        new SingleStageJoinStreamOperator(joinedTable.getTableName(), false,
                                broadcastChainJoinInput, JoinAlgorithm.BROADCAST_CHAIN);
            }
        }
        else
        {
            childOperator = getJoinOperator((JoinedTable) leftTable, Optional.of(joinedTable));
            requireNonNull(childOperator, "failed to get child operator");
            // check if there is an incomplete chain join.
            if (childOperator.getJoinAlgo() == JoinAlgorithm.BROADCAST_CHAIN &&
                    !childOperator.isComplete() && // Issue #482: ensure the child operator is complete
                    joinAlgo == JoinAlgorithm.BROADCAST && join.getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                // the current join is still a broadcast join and the left child is an incomplete chain join.
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
                        int numPartition = PlanOptimizer.Instance().getJoinNumPartition(
                                this.transId,
                                parent.get().getJoin().getLeftTable(),
                                parent.get().getJoin().getRightTable(),
                                parent.get().getJoin().getJoinEndian());

                        /*
                        * Issue #484:
                        * After we supported multi-pipeline join, this method might be called from getMultiPipelineJoinOperator().
                        * Therefore, the current join might be either the left child or the right child of the parent, and we must
                        * decide whether to use the left key column ids or the right key column ids of the parent as the post
                        * partitioning key column ids.
                        * */
                        if (joinedTable == parent.get().getJoin().getLeftTable())
                        {
                            postPartitionInfo = new PartitionInfo(parent.get().getJoin().getLeftKeyColumnIds(), numPartition);
                        }
                        else
                        {
                            postPartitionInfo = new PartitionInfo(parent.get().getJoin().getRightKeyColumnIds(), numPartition);
                        }

                        /*
                         * For broadcast and broadcast chain join, if every worker in its parent has to
                         * read the outputs of this join, we try to adjust the input splits of its large table.
                         *
                         * If the parent is a large-left broadcast join (small-left broadcast join does not go into
                         * this branch). The outputs of this join will be split into multiple workers and there is
                         * no need to adjust the input splits for this join.
                         */
                        rightInputSplits = adjustInputSplitsForBroadcastJoin(leftTable, rightTable, rightInputSplits);
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
                        ImmutableList<String> outputs = ImmutableList.of((outputId++) + "/join");
                        for (int j = 0; j < IntraWorkerParallelism && i < rightInputSplits.size(); ++j, ++i)
                        {
                            inputsBuilder.add(rightInputSplits.get(i));
                        }
                        BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                                rightTable, inputsBuilder.build(), join.getRightKeyColumnIds());

                        String path = getIntermediateFolderForTrans(transId) + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/";
                        MultiOutputInfo output = new MultiOutputInfo(path, IntermediateStorageInfo, true, outputs);

                        BroadcastChainJoinInput complete = broadcastChainJoinInput.toBuilder()
                                .setLargeTable(rightTableInfo)
                                .setJoinInfo(joinInfo)
                                .setOutput(output).build();

                        joinInputs.add(complete);
                    }

                    return EnabledExchangeMethod == ExchangeMethod.batch ?
                            new SingleStageJoinBatchOperator(joinedTable.getTableName(), true,
                                    joinInputs.build(), JoinAlgorithm.BROADCAST_CHAIN) :
                            new SingleStageJoinStreamOperator(joinedTable.getTableName(), true,
                                    joinInputs.build(), JoinAlgorithm.BROADCAST_CHAIN);
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
                int numPartition = PlanOptimizer.Instance().getJoinNumPartition(
                        this.transId,
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
            if (leftTable.getTableType() == Table.TableType.BASE && ((BaseTable) leftTable).getFilter().isEmpty() &&
                    rightTable.getFilter().isEmpty())// && (leftInputSplits.size() <= 128 && rightInputSplits.size() <= 128))
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

                if (parent.isPresent() && (parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED ||
                        (parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST &&
                                parent.get().getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)))
                {
                    /*
                     * For broadcast and broadcast chain join, if every worker in its parent has to
                     * read the outputs of this join, we try to adjust the input splits of its large table.
                     *
                     * This join is the left child of its parent, therefore, if the parent is a partitioned
                     * join or small-left broadcast join, the outputs of this join will be read by every
                     * worker of the parent.
                     */
                    rightInputSplits = adjustInputSplitsForBroadcastJoin(leftTable, rightTable, rightInputSplits);
                }

                int outputId = 0;
                for (int i = 0; i < rightInputSplits.size();)
                {
                    ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                            .builderWithExpectedSize(internalParallelism);
                    ImmutableList<String> outputs = ImmutableList.of((outputId++) + "/join");
                    for (int j = 0; j < internalParallelism && i < rightInputSplits.size(); ++j, ++i)
                    {
                        inputsBuilder.add(rightInputSplits.get(i));
                    }
                    BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                            rightTable, inputsBuilder.build(), join.getRightKeyColumnIds());

                    String path = getIntermediateFolderForTrans(transId) + joinedTable.getSchemaName() + "/" +
                            joinedTable.getTableName() + "/";
                    MultiOutputInfo output = new MultiOutputInfo(path, IntermediateStorageInfo, true, outputs);

                    BroadcastJoinInput joinInput = new BroadcastJoinInput(
                            transId, leftTableInfo, rightTableInfo, joinInfo,
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

                if (parent.isPresent() && (parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED ||
                        (parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST &&
                                parent.get().getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)))
                {
                    /*
                     * For broadcast and broadcast chain join, if every worker in its parent has to
                     * read the outputs of this join, we try to adjust the input splits of its large table.
                     *
                     * This join is the left child of its parent, therefore, if the parent is a partitioned
                     * join or small-left broadcast join, the outputs of this join will be read by every
                     * worker of the parent.
                     */
                    leftInputSplits = adjustInputSplitsForBroadcastJoin(rightTable, leftTable, leftInputSplits);
                }

                int outputId = 0;
                for (int i = 0; i < leftInputSplits.size();)
                {
                    ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                            .builderWithExpectedSize(internalParallelism);
                    ImmutableList<String> outputs = ImmutableList.of((outputId++) + "/join");
                    for (int j = 0; j < internalParallelism && i < leftInputSplits.size(); ++j, ++i)
                    {
                        inputsBuilder.add(leftInputSplits.get(i));
                    }
                    BroadcastTableInfo leftTableInfo = getBroadcastTableInfo(
                            leftTable, inputsBuilder.build(), join.getLeftKeyColumnIds());

                    String path = getIntermediateFolderForTrans(transId) + joinedTable.getSchemaName() + "/" +
                            joinedTable.getTableName() + "/";
                    MultiOutputInfo output = new MultiOutputInfo(path, IntermediateStorageInfo, true, outputs);

                    BroadcastJoinInput joinInput = new BroadcastJoinInput(
                            transId, rightTableInfo, leftTableInfo, joinInfo,
                            false, null, output);

                    joinInputs.add(joinInput);
                }
            }
            SingleStageJoinOperator joinOperator = EnabledExchangeMethod == ExchangeMethod.batch ?
                    new SingleStageJoinBatchOperator(joinedTable.getTableName(),
                            true, joinInputs.build(), joinAlgo) :
                    new SingleStageJoinStreamOperator(joinedTable.getTableName(),
                            true, joinInputs.build(), joinAlgo);
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
            int numPartition = PlanOptimizer.Instance().getJoinNumPartition(
                    this.transId, leftTable, rightTable, join.getJoinEndian());
            if (childOperator != null)
            {
                // left side is post partitioned, thus we only partition the right table.
                boolean leftIsBase = leftTable.getTableType() == Table.TableType.BASE;
                PartitionedTableInfo leftTableInfo = new PartitionedTableInfo(
                        leftTable.getTableName(), leftIsBase, leftTable.getColumnNames(),
                        leftIsBase ? InputStorageInfo : IntermediateStorageInfo,
                        leftPartitionedFiles, IntraWorkerParallelism, leftKeyColumnIds);

                boolean[] rightPartitionProjection = getPartitionProjection(rightTable, join.getRightProjection());

                List<PartitionInput> rightPartitionInputs = getPartitionInputs(
                        rightTable, rightInputSplits, rightKeyColumnIds, rightPartitionProjection, numPartition,
                        getIntermediateFolderForTrans(transId) + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/" + rightTable.getTableName() + "/");

                PartitionedTableInfo rightTableInfo = getPartitionedTableInfo(
                        rightTable, rightKeyColumnIds, rightPartitionInputs, rightPartitionProjection);

                List<JoinInput> joinInputs = getPartitionedJoinInputs(
                        joinedTable, parent, numPartition, leftTableInfo, rightTableInfo,
                        null, rightPartitionProjection);

                if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    joinOperator = EnabledExchangeMethod == ExchangeMethod.batch ?
                            new PartitionedJoinBatchOperator(joinedTable.getTableName(),
                                    null, rightPartitionInputs, joinInputs, joinAlgo) :
                            new PartitionedJoinStreamOperator(joinedTable.getTableName(),
                                    null, rightPartitionInputs, joinInputs, joinAlgo);
                    joinOperator.setSmallChild(childOperator);
                }
                else
                {
                    joinOperator = EnabledExchangeMethod == ExchangeMethod.batch ?
                            new PartitionedJoinBatchOperator(joinedTable.getTableName(),
                                    rightPartitionInputs, null, joinInputs, joinAlgo) :
                            new PartitionedJoinStreamOperator(joinedTable.getTableName(),
                                    null, rightPartitionInputs, joinInputs, joinAlgo);
                    joinOperator.setLargeChild(childOperator);
                }
            }
            else
            {
                // partition both tables. in this case, the operator's child must be null.
                boolean[] leftPartitionProjection = getPartitionProjection(leftTable, join.getLeftProjection());
                List<PartitionInput> leftPartitionInputs = getPartitionInputs(
                        leftTable, leftInputSplits, leftKeyColumnIds, leftPartitionProjection, numPartition,
                        getIntermediateFolderForTrans(transId) + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/" + leftTable.getTableName() + "/");
                PartitionedTableInfo leftTableInfo = getPartitionedTableInfo(
                        leftTable, leftKeyColumnIds, leftPartitionInputs, leftPartitionProjection);

                boolean[] rightPartitionProjection = getPartitionProjection(rightTable, join.getRightProjection());
                List<PartitionInput> rightPartitionInputs = getPartitionInputs(
                        rightTable, rightInputSplits, rightKeyColumnIds, rightPartitionProjection, numPartition,
                        getIntermediateFolderForTrans(transId) + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/" + rightTable.getTableName() + "/");
                PartitionedTableInfo rightTableInfo = getPartitionedTableInfo(
                        rightTable, rightKeyColumnIds, rightPartitionInputs, rightPartitionProjection);

                List<JoinInput> joinInputs = getPartitionedJoinInputs(
                        joinedTable, parent, numPartition, leftTableInfo, rightTableInfo,
                        leftPartitionProjection, rightPartitionProjection);

                if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    joinOperator = EnabledExchangeMethod == ExchangeMethod.batch ?
                            new PartitionedJoinBatchOperator(joinedTable.getTableName(),
                                    leftPartitionInputs, rightPartitionInputs, joinInputs, joinAlgo) :
                            new PartitionedJoinStreamOperator(joinedTable.getTableName(),
                                    leftPartitionInputs, rightPartitionInputs, joinInputs, joinAlgo);
                }
                else
                {
                    joinOperator = EnabledExchangeMethod == ExchangeMethod.batch ?
                            new PartitionedJoinBatchOperator(joinedTable.getTableName(),
                                    rightPartitionInputs, leftPartitionInputs, joinInputs, joinAlgo) :
                            new PartitionedJoinStreamOperator(joinedTable.getTableName(),
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
        tableInfo.setInputSplits(inputSplits);
        tableInfo.setColumnsToRead(table.getColumnNames());
        if (table.getTableType() == Table.TableType.BASE)
        {
            tableInfo.setFilter(JSON.toJSONString(((BaseTable) table).getFilter()));
            tableInfo.setBase(true);
            tableInfo.setStorageInfo(InputStorageInfo);
        }
        else
        {
            tableInfo.setFilter(JSON.toJSONString(
                    TableScanFilter.empty(table.getSchemaName(), table.getTableName())));
            tableInfo.setBase(false);
            tableInfo.setStorageInfo(IntermediateStorageInfo);
        }
        tableInfo.setKeyColumnIds(keyColumnIds);
        return tableInfo;
    }

    /**
     * Get the partition projection for the base table that is to be partitioned.
     * If a column only exists in the filters but does not exist in the join projection
     * (corresponding element is false), then the corresponding element in the partition
     * projection is false. Otherwise, it is true.
     *
     * @param table the base table
     * @param joinProjection the join projection
     * @return the partition projection for the partition operator
     */
    private boolean[] getPartitionProjection(Table table, boolean[] joinProjection)
    {
        String[] columnsToRead = table.getColumnNames();
        boolean[] projection = new boolean[columnsToRead.length];
        if (table.getTableType() == Table.TableType.BASE)
        {
            TableScanFilter tableScanFilter = ((BaseTable)table).getFilter();
            Set<Integer> filterColumnIds = tableScanFilter.getColumnFilters().keySet();
            for (int i = 0; i < columnsToRead.length; ++i)
            {
                if (!joinProjection[i] && filterColumnIds.contains(i))
                {
                    checkArgument(columnsToRead[i].equals(tableScanFilter.getColumnFilter(i).getColumnName()),
                            "the column name in the table and the filter do not match");
                    projection[i] = false;
                } else
                {
                    projection[i] = true;
                }
            }
        }
        else
        {
            Arrays.fill(projection, true);
        }
        return projection;
    }

    private String[] rewriteColumnsToReadForPartitionedJoin(String[] originColumnsToRead, boolean[] partitionProjection)
    {
        requireNonNull(originColumnsToRead, "originColumnsToRead is null");
        requireNonNull(partitionProjection, "partitionProjection is null");
        checkArgument(originColumnsToRead.length == partitionProjection.length,
                "originColumnsToRead and partitionProjection are not of the same length");
        int len = 0;
        for (boolean b : partitionProjection)
        {
            if (b)
            {
                len++;
            }
        }
        if (len == partitionProjection.length)
        {
            return originColumnsToRead;
        }

        String[] columnsToRead = new String[len];
        for (int i = 0, j = 0; i < partitionProjection.length; ++i)
        {
            if (partitionProjection[i])
            {
                columnsToRead[j++] = originColumnsToRead[i];
            }
        }
        return columnsToRead;
    }

    private boolean[] rewriteProjectionForPartitionedJoin(boolean[] originProjection, boolean[] partitionProjection)
    {
        requireNonNull(originProjection, "originProjection is null");
        requireNonNull(partitionProjection, "partitionProjection is null");
        checkArgument(originProjection.length == partitionProjection.length,
                "originProjection and partitionProjection are not of the same length");
        int len = 0;
        for (boolean b : partitionProjection)
        {
            if (b)
            {
                len++;
            }
        }
        if (len == partitionProjection.length)
        {
            return originProjection;
        }

        boolean[] projection = new boolean[len];
        for (int i = 0, j = 0; i < partitionProjection.length; ++i)
        {
            if (partitionProjection[i])
            {
                projection[j++] = originProjection[i];
            }
        }
        return projection;
    }

    private int[] rewriteColumnIdsForPartitionedJoin(int[] originColumnIds, boolean[] partitionProjection)
    {
        requireNonNull(originColumnIds, "originProjection is null");
        requireNonNull(partitionProjection, "partitionProjection is null");
        checkArgument(originColumnIds.length <= partitionProjection.length,
                "originColumnIds has more elements than partitionProjection");
        Map<Integer, Integer> columnIdMap = new HashMap<>();
        for (int i = 0, j = 0; i < partitionProjection.length; ++i)
        {
            if (partitionProjection[i])
            {
                columnIdMap.put(i, j++);
            }
        }
        if (columnIdMap.size() == partitionProjection.length)
        {
            return originColumnIds;
        }

        int[] columnIds = new int[originColumnIds.length];
        for (int i = 0; i < originColumnIds.length; ++i)
        {
            columnIds[i] = columnIdMap.get(originColumnIds[i]);
        }
        return columnIds;
    }

    private PartitionedTableInfo getPartitionedTableInfo(
            Table table, int[] keyColumnIds, List<PartitionInput> partitionInputs, boolean[] partitionProjection)
    {
        ImmutableList.Builder<String> rightPartitionedFiles = ImmutableList.builder();
        for (PartitionInput partitionInput : partitionInputs)
        {
            rightPartitionedFiles.add(partitionInput.getOutput().getPath());
        }

        int[] newKeyColumnIds = rewriteColumnIdsForPartitionedJoin(keyColumnIds, partitionProjection);
        String[] newColumnsToRead = rewriteColumnsToReadForPartitionedJoin(table.getColumnNames(), partitionProjection);

        if (table.getTableType() == Table.TableType.BASE)
        {
            return new PartitionedTableInfo(table.getTableName(), true,
                    newColumnsToRead, InputStorageInfo, rightPartitionedFiles.build(),
                    IntraWorkerParallelism, newKeyColumnIds);
        } else
        {
            return new PartitionedTableInfo(table.getTableName(), false,
                    newColumnsToRead, IntermediateStorageInfo, rightPartitionedFiles.build(),
                    IntraWorkerParallelism, newKeyColumnIds);
        }
    }

    private List<PartitionInput> getPartitionInputs(Table inputTable, List<InputSplit> inputSplits, int[] keyColumnIds,
                                                    boolean[] partitionProjection, int numPartition, String outputBase)
    {
        ImmutableList.Builder<PartitionInput> partitionInputsBuilder = ImmutableList.builder();
        int outputId = 0;
        for (int i = 0; i < inputSplits.size();)
        {
            PartitionInput partitionInput = new PartitionInput();
            partitionInput.setTransId(transId);
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
            if (inputTable.getTableType() == Table.TableType.BASE)
            {
                tableInfo.setFilter(JSON.toJSONString(((BaseTable) inputTable).getFilter()));
                tableInfo.setBase(true);
                tableInfo.setStorageInfo(InputStorageInfo);
            }
            else
            {
                tableInfo.setFilter(JSON.toJSONString(
                        TableScanFilter.empty(inputTable.getSchemaName(), inputTable.getTableName())));
                tableInfo.setBase(false);
                tableInfo.setStorageInfo(IntermediateStorageInfo);
            }
            partitionInput.setTableInfo(tableInfo);
            partitionInput.setProjection(partitionProjection);
            partitionInput.setOutput(new OutputInfo(outputBase + (outputId++) + "/part", InputStorageInfo, true));
            int[] newKeyColumnIds = rewriteColumnIdsForPartitionedJoin(keyColumnIds, partitionProjection);
            partitionInput.setPartitionInfo(new PartitionInfo(newKeyColumnIds, numPartition));
            partitionInputsBuilder.add(partitionInput);
        }

        return partitionInputsBuilder.build();
    }

    /**
     * Get the join inputs of a partitioned join, given the left and right partitioned tables.
     * @param joinedTable this joined table
     * @param parent the parent of this joined table
     * @param numPartition the number of partitions
     * @param leftTableInfo the left partitioned table
     * @param rightTableInfo the right partitioned table
     * @param leftPartitionProjection the partition projection of the left table, null if not exists
     * @param rightPartitionProjection the partition projection of the right table, null if not exists
     * @return the join input of this join
     * @throws MetadataException
     */
    private List<JoinInput> getPartitionedJoinInputs(
            JoinedTable joinedTable, Optional<JoinedTable> parent, int numPartition,
            PartitionedTableInfo leftTableInfo, PartitionedTableInfo rightTableInfo,
            boolean[] leftPartitionProjection, boolean[] rightPartitionProjection)
            throws MetadataException, InvalidProtocolBufferException
    {
        boolean postPartition = false;
        PartitionInfo postPartitionInfo = null;
        if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
        {
            postPartition = true;
            // Note: DO NOT use numPartition as the number of partitions for post partitioning.
            int numPostPartition = PlanOptimizer.Instance().getJoinNumPartition(
                    this.transId,
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
            outputFileNames.add(i + "/join");
            if (joinedTable.getJoin().getJoinType() == JoinType.EQUI_LEFT ||
                    joinedTable.getJoin().getJoinType() == JoinType.EQUI_FULL)
            {
                outputFileNames.add(i + "/join_left");
            }

            String path = getIntermediateFolderForTrans(transId) + joinedTable.getSchemaName() + "/" +
                    joinedTable.getTableName() + "/";
            MultiOutputInfo output = new MultiOutputInfo(path, IntermediateStorageInfo, true, outputFileNames.build());

            boolean[] leftProjection = leftPartitionProjection == null ? joinedTable.getJoin().getLeftProjection() :
                    rewriteProjectionForPartitionedJoin(joinedTable.getJoin().getLeftProjection(), leftPartitionProjection);
            boolean[] rightProjection = rightPartitionProjection == null ? joinedTable.getJoin().getRightProjection() :
                    rewriteProjectionForPartitionedJoin(joinedTable.getJoin().getRightProjection(), rightPartitionProjection);

            PartitionedJoinInput joinInput;
            if (joinedTable.getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo(joinedTable.getJoin().getJoinType(),
                        joinedTable.getJoin().getLeftColumnAlias(), joinedTable.getJoin().getRightColumnAlias(),
                        leftProjection, rightProjection, postPartition, postPartitionInfo, numPartition, ImmutableList.of(i));
                 joinInput = new PartitionedJoinInput(transId, leftTableInfo, rightTableInfo, joinInfo,
                         false, null, output);
            }
            else
            {
                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo(joinedTable.getJoin().getJoinType().flip(),
                        joinedTable.getJoin().getRightColumnAlias(), joinedTable.getJoin().getLeftColumnAlias(),
                        rightProjection, leftProjection, postPartition, postPartitionInfo, numPartition, ImmutableList.of(i));
                joinInput = new PartitionedJoinInput(transId, rightTableInfo, leftTableInfo, joinInfo,
                        false, null, output);
            }

            joinInputs.add(joinInput);
        }
        return joinInputs.build();
    }

    /**
     * Adjust the input splits for a broadcast join, if the output of this join is read by its parent.
     * The adjustment retries to reduce the number of workers in this join according to the selectivity
     * of the small table in this join.
     * @param smallTable the small table in this join
     * @param largeTable the large table in this join
     * @param largeInputSplits the input splits for the large table
     * @return the adjusted input splits for the large table, i.e., this join
     * @throws InvalidProtocolBufferException
     * @throws MetadataException
     */
    private List<InputSplit> adjustInputSplitsForBroadcastJoin(
            Table smallTable, Table largeTable, List<InputSplit> largeInputSplits)
            throws InvalidProtocolBufferException, MetadataException
    {
        int numWorkers = largeInputSplits.size() / IntraWorkerParallelism;
        if (numWorkers <= 32)
        {
            // There are less than 32 workers, they are not likely to affect the performance.
            return largeInputSplits;
        }
        double smallSelectivity = PlanOptimizer.Instance().getTableSelectivity(this.transId, smallTable);
        double largeSelectivity = PlanOptimizer.Instance().getTableSelectivity(this.transId, largeTable);
        if (smallSelectivity >= 0 && largeSelectivity > 0 && smallSelectivity < largeSelectivity)
        {
            // Adjust the input split size if the small table has a lower selectivity.
            int numSplits = largeInputSplits.size();
            int numInputInfos = 0;
            ImmutableList.Builder<InputInfo> inputInfosBuilder = ImmutableList.builder();
            for (InputSplit inputSplit : largeInputSplits)
            {
                numInputInfos += inputSplit.getInputInfos().size();
                inputInfosBuilder.addAll(inputSplit.getInputInfos());
            }
            int inputInfosPerSplit = numInputInfos / numSplits;
            if (numInputInfos % numSplits > 0)
            {
                inputInfosPerSplit++;
            }
            if (smallSelectivity / largeSelectivity < 0.25)
            {
                // Do not adjust too aggressively.
                logger.debug("increasing the split size of table '" + largeTable.getTableName() +
                        "' by factor of 2");
                inputInfosPerSplit *= 2;
            }
            else
            {
                return largeInputSplits;
            }
            List<InputInfo> inputInfos = inputInfosBuilder.build();
            ImmutableList.Builder<InputSplit> inputSplitsBuilder = ImmutableList.builder();
            for (int i = 0; i < numInputInfos; )
            {
                ImmutableList.Builder<InputInfo> builder = ImmutableList.builderWithExpectedSize(inputInfosPerSplit);
                for (int j = 0; j < inputInfosPerSplit && i < numInputInfos; ++j, ++i)
                {
                    builder.add(inputInfos.get(i));
                }
                inputSplitsBuilder.add(new InputSplit(builder.build()));
            }
            return inputSplitsBuilder.build();
        }
        else
        {
            return largeInputSplits;
        }
    }

    private List<InputSplit> getInputSplits(BaseTable table) throws MetadataException, IOException
    {
        requireNonNull(table, "table is null");
        checkArgument(table.getTableType() == Table.TableType.BASE, "this is not a base table");
        ImmutableList.Builder<InputSplit> splitsBuilder = ImmutableList.builder();
        int splitSize = 0;
        Storage.Scheme tableStorageScheme =
                metadataService.getTable(table.getSchemaName(), table.getTableName()).getStorageScheme();
        checkArgument(tableStorageScheme.equals(this.storage.getScheme()), String.format(
                "the storage scheme of table '%s.%s' is not consistent with the input storage scheme for Pixels Turbo",
                table.getSchemaName(), table.getTableName()));
        // Issue #506: add the column size of the base table to the scan size of this query.
        List<Column> columns = metadataService.getColumns(table.getSchemaName(), table.getTableName(), true);
        Map<String, Column> nameToColumnMap = new HashMap<>(columns.size());
        for (Column column : columns)
        {
            nameToColumnMap.put(column.getName(), column);
        }
        for (String columnName : table.getColumnNames())
        {
            this.scanSize += nameToColumnMap.get(columnName).getSize();
        }

        List<Layout> layouts = metadataService.getLayouts(table.getSchemaName(), table.getTableName());
        for (Layout layout : layouts)
        {
            long version = layout.getVersion();
            SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
            Ordered ordered = layout.getOrdered();
            ColumnSet columnSet = new ColumnSet();
            for (String column : table.getColumnNames())
            {
                columnSet.addColumn(column);
            }

            // get split size
            Splits splits = layout.getSplits();
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
                    splitsIndex = buildSplitsIndex(version, ordered, splits, schemaTableName);
                } else
                {
                    long indexVersion = splitsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("splits index version is not up-to-date, updating index...");
                        splitsIndex = buildSplitsIndex(version, ordered, splits, schemaTableName);
                    }
                }
                SplitPattern bestSplitPattern = splitsIndex.search(columnSet);
                splitSize = bestSplitPattern.getSplitSize();
                logger.debug("split size for table '" + table.getTableName() + "': " + splitSize + " from splits index");
                double selectivity = PlanOptimizer.Instance().getTableSelectivity(this.transId, table);
                if (selectivity >= 0)
                {
                    // Increasing split size according to the selectivity.
                    if (selectivity < 0.25)
                    {
                        splitSize *= 4;
                    } else if (selectivity < 0.5)
                    {
                        splitSize *= 2;
                    }
                    if (splitSize > splitsIndex.getMaxSplitSize())
                    {
                        splitSize = splitsIndex.getMaxSplitSize();
                    }
                }
                logger.debug("split size for table '" + table.getTableName() + "': " + splitSize + " after adjustment");
            }
            logger.debug("using split size: " + splitSize);
            int rowGroupNum = splits.getNumRowGroupInFile();

            // get compact path
            String[] compactPaths;
            if (projectionReadEnabled)
            {
                ProjectionsIndex projectionsIndex = IndexFactory.Instance().getProjectionsIndex(schemaTableName);
                Projections projections = layout.getProjections();
                if (projectionsIndex == null)
                {
                    logger.debug("projections index not exist in factory, building index...");
                    projectionsIndex = buildProjectionsIndex(ordered, projections, schemaTableName);
                }
                else
                {
                    int indexVersion = projectionsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("projections index is not up-to-date, updating index...");
                        projectionsIndex = buildProjectionsIndex(ordered, projections, schemaTableName);
                    }
                }
                ProjectionPattern projectionPattern = projectionsIndex.search(columnSet);
                if (projectionPattern != null)
                {
                    logger.debug("suitable projection pattern is found, pathIds='" + Arrays.toString(projectionPattern.getPathIds()) + '\'');
                    long[] projectionPathIds = projectionPattern.getPathIds();
                    Map<Long, Path> projectionPaths = layout.getProjectionPaths();
                    compactPaths = new String[projectionPathIds.length];
                    for (int i = 0; i < projectionPathIds.length; ++i)
                    {
                        compactPaths[i] = projectionPaths.get(projectionPathIds[i]).getUri();
                    }
                }
                else
                {
                    compactPaths = layout.getCompactPathUris();
                }
            }
            else
            {
                compactPaths = layout.getCompactPathUris();
            }
            logger.debug("using compact path: " + Joiner.on(";").join(compactPaths));

            // get the inputs from storage
            try
            {
                // 1. add splits in orderedPath
                if (orderedPathEnabled)
                {
                    List<String> orderedPaths = storage.listPaths(layout.getOrderedPathUris());

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
                    List<String> compactFilePaths = storage.listPaths(compactPaths);

                    int curFileRGIdx;
                    for (String path : compactFilePaths)
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

    private SplitsIndex buildSplitsIndex(long version, Ordered ordered, Splits splits, SchemaTableName schemaTableName)
            throws MetadataException
    {
        List<String> columnOrder = ordered.getColumnOrder();
        SplitsIndex index;
        String indexTypeName = ConfigFactory.Instance().getProperty("splits.index.type");
        SplitsIndex.IndexType indexType = SplitsIndex.IndexType.valueOf(indexTypeName.toUpperCase());
        switch (indexType)
        {
            case INVERTED:
                index = new InvertedSplitsIndex(version, columnOrder,
                        SplitPattern.buildPatterns(columnOrder, splits), splits.getNumRowGroupInFile());
                break;
            case COST_BASED:
                index = new CostBasedSplitsIndex(this.transId, version, this.metadataService, schemaTableName,
                        splits.getNumRowGroupInFile(), splits.getNumRowGroupInFile());
                break;
            default:
                throw new UnsupportedOperationException("splits index type '" + indexType + "' is not supported");
        }

        IndexFactory.Instance().cacheSplitsIndex(schemaTableName, index);
        return index;
    }

    private ProjectionsIndex buildProjectionsIndex(Ordered ordered, Projections projections, SchemaTableName schemaTableName)
    {
        List<String> columnOrder = ordered.getColumnOrder();
        ProjectionsIndex index;
        index = new InvertedProjectionsIndex(columnOrder, ProjectionPattern.buildPatterns(columnOrder, projections));
        IndexFactory.Instance().cacheProjectionsIndex(schemaTableName, index);
        return index;
    }
}
