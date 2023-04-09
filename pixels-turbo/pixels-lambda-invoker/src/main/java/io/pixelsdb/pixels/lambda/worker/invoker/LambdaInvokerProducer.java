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
package io.pixelsdb.pixels.lambda.worker.invoker;

import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProducer;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

/**
 * @author hank
 * @date 2023.04.06
 */
public class LambdaInvokerProducer implements InvokerProducer
{
    private static final ConfigFactory config = ConfigFactory.Instance();

    public Invoker produce(WorkerType workerType)
    {
        switch (workerType)
        {
            case SCAN:
                String scanWorker = config.getProperty("scan.worker.name");
                return new ScanInvoker(scanWorker);
            case PARTITION:
                String partitionWorker = config.getProperty("partition.worker.name");
                return new PartitionInvoker(partitionWorker);
            case BROADCAST_JOIN:
                String broadcastJoinWorker = config.getProperty("broadcast.join.worker.name");
                return new BroadcastJoinInvoker(broadcastJoinWorker);
            case BROADCAST_CHAIN_JOIN:
                String broadcastChainJoinWorker = config.getProperty("broadcast.chain.join.worker.name");
                return new BroadcastChainJoinInvoker(broadcastChainJoinWorker);
            case PARTITIONED_JOIN:
                String partitionedJoinWorker = config.getProperty("partitioned.join.worker.name");
                return new PartitionedJoinInvoker(partitionedJoinWorker);
            case PARTITIONED_CHAIN_JOIN:
                String partitionedChainJoinWorker = config.getProperty("partitioned.chain.join.worker.name");
                return new PartitionedChainJoinInvoker(partitionedChainJoinWorker);
            case AGGREGATION:
                String aggregationWorker = config.getProperty("aggregation.worker.name");
                return new AggregationInvoker(aggregationWorker);
            default:
                return null;
        }
    }
}
