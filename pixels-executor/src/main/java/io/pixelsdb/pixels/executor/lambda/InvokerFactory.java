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

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author hank
 * @date 6/28/22
 */
public class InvokerFactory
{
    private static final InvokerFactory instance = new InvokerFactory();

    public static InvokerFactory Instance()
    {
        return instance;
    }

    private final Map<WorkerType, Invoker> invokerMap = new HashMap<>();

    private InvokerFactory()
    {
        ConfigFactory config = ConfigFactory.Instance();

        String scanWorker = config.getProperty("scan.worker.name");
        invokerMap.put(WorkerType.SCAN, new ScanInvoker(scanWorker));

        String partitionWorker = config.getProperty("partition.worker.name");
        invokerMap.put(WorkerType.PARTITION, new PartitionInvoker(partitionWorker));

        String broadcastJoinWorker = config.getProperty("broadcast.join.worker.name");
        invokerMap.put(WorkerType.BROADCAST_JOIN, new BroadcastJoinInvoker(broadcastJoinWorker));

        String broadcastChainJoinWorker = config.getProperty("broadcast.chain.join.worker.name");
        invokerMap.put(WorkerType.BROADCAST_CHAIN_JOIN, new BroadcastChainJoinInvoker(broadcastChainJoinWorker));

        String partitionedJoinWorker = config.getProperty("partitioned.join.worker.name");
        invokerMap.put(WorkerType.PARTITIONED_JOIN, new PartitionedJoinInvoker(partitionedJoinWorker));

        String partitionedChainJoinWorker = config.getProperty("partitioned.chain.join.worker.name");
        invokerMap.put(WorkerType.PARTITIONED_CHAIN_JOIN, new PartitionedChainJoinInvoker(partitionedChainJoinWorker));

        String aggregationWorker = config.getProperty("aggregation.worker.name");
        invokerMap.put(WorkerType.AGGREGATION, new AggregationInvoker(aggregationWorker));
    }

    public Invoker getInvoker(WorkerType workerType)
    {
        return this.invokerMap.get(workerType);
    }
}
