/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical;

import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * S3QS exchange scheduler for partitioned joins.
 *
 * Unlike HTTP stream exchange, the workers are the normal partition and
 * partitioned-join workers. The pipeline boundary is the S3QS shuffle protocol:
 * producers publish DATA/PRODUCER_END messages, while consumers poll the queues.
 */
public class PartitionedJoinS3QSOperator extends PartitionedJoinOperator
{
    private static final Logger logger = LogManager.getLogger(PartitionedJoinS3QSOperator.class);
    private static final CompletableFuture<Void> Completed = CompletableFuture.completedFuture(null);

    public PartitionedJoinS3QSOperator(String name, List<PartitionInput> smallPartitionInputs,
                                       List<PartitionInput> largePartitionInputs,
                                       List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        super(name, smallPartitionInputs, largePartitionInputs, joinInputs, joinAlgo);
    }

    @Override
    public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
    {
        return executePrev().handle((result, exception) ->
        {
            if (exception != null)
            {
                throw new CompletionException("failed to start the previous S3QS stages", exception);
            }
            validateJoinInputs();
            joinOutputs = new CompletableFuture[joinInputs.size()];
            for (int i = 0; i < joinInputs.size(); ++i)
            {
                JoinInput joinInput = joinInputs.get(i);
                if (joinAlgo == JoinAlgorithm.PARTITIONED)
                {
                    joinOutputs[i] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITIONED_JOIN).invoke(joinInput);
                }
                else if (joinAlgo == JoinAlgorithm.PARTITIONED_CHAIN)
                {
                    joinOutputs[i] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITIONED_CHAIN_JOIN).invoke(joinInput);
                }
                else
                {
                    throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is unsupported");
                }
            }

            logger.debug("invoke S3QS join " + this.getName());
            return joinOutputs;
        });
    }

    @Override
    public CompletableFuture<Void> executePrev()
    {
        validatePartitionInputs();
        if (smallChild != null && largeChild != null)
        {
            checkArgument(smallPartitionInputs.isEmpty(), "smallPartitionInputs is not empty");
            checkArgument(largePartitionInputs.isEmpty(), "largePartitionInputs is not empty");
            smallChild.execute();
            largeChild.execute();
        }
        else if (smallChild != null)
        {
            checkArgument(smallPartitionInputs.isEmpty(), "smallPartitionInputs is not empty");
            checkArgument(!largePartitionInputs.isEmpty(), "largePartitionInputs is empty");
            smallChild.execute();
            largePartitionOutputs = invokePartitionWorkers(largePartitionInputs);
            logger.debug("invoke large S3QS partition of " + this.getName());
        }
        else if (largeChild != null)
        {
            checkArgument(!smallPartitionInputs.isEmpty(), "smallPartitionInputs is empty");
            checkArgument(largePartitionInputs.isEmpty(), "largePartitionInputs is not empty");
            smallPartitionOutputs = invokePartitionWorkers(smallPartitionInputs);
            logger.debug("invoke small S3QS partition of " + this.getName());
            largeChild.execute();
        }
        else
        {
            checkArgument(!smallPartitionInputs.isEmpty(), "smallPartitionInputs is empty");
            checkArgument(!largePartitionInputs.isEmpty(), "largePartitionInputs is empty");
            smallPartitionOutputs = invokePartitionWorkers(smallPartitionInputs);
            logger.debug("invoke small S3QS partition of " + this.getName());
            largePartitionOutputs = invokePartitionWorkers(largePartitionInputs);
            logger.debug("invoke large S3QS partition of " + this.getName());
        }
        return Completed;
    }

    private CompletableFuture<? extends Output>[] invokePartitionWorkers(List<PartitionInput> partitionInputs)
    {
        CompletableFuture<? extends Output>[] outputs = new CompletableFuture[partitionInputs.size()];
        int i = 0;
        for (PartitionInput partitionInput : partitionInputs)
        {
            outputs[i++] = InvokerFactory.Instance()
                    .getInvoker(WorkerType.PARTITION).invoke(partitionInput);
        }
        return outputs;
    }

    private void validatePartitionInputs()
    {
        if (!smallPartitionInputs.isEmpty())
        {
            checkS3QSPartitionInputs(smallPartitionInputs, "small");
        }
        if (!largePartitionInputs.isEmpty())
        {
            checkS3QSPartitionInputs(largePartitionInputs, "large");
        }
    }

    private void checkS3QSPartitionInputs(List<PartitionInput> partitionInputs, String side)
    {
        for (PartitionInput partitionInput : partitionInputs)
        {
            checkArgument(partitionInput.getOutput() != null,
                    "%s partition input does not have output info", side);
            checkS3QSShuffleInfo(partitionInput.getOutput().getShuffleInfo(), side + " partition input");
            checkArgument(partitionInput.getProducerTaskId() >= 0,
                    "%s partition input does not have producerTaskId", side);
        }
    }

    static void checkS3QSShuffleInfo(ShuffleInfo shuffleInfo, String source)
    {
        checkArgument(isS3QSShuffle(shuffleInfo),
                "%s is not an explicit S3QS shuffle", source);
        checkArgument(!isNullOrEmpty(shuffleInfo.getShuffleId()),
                "%s does not have shuffleId", source);
        checkArgument(!isNullOrEmpty(shuffleInfo.getObjectPathPrefix()),
                "%s does not have objectPathPrefix", source);
        checkArgument(shuffleInfo.getNumPartitions() > 0,
                "%s does not have positive numPartitions", source);
        checkArgument(shuffleInfo.getProducerCount() > 0,
                "%s does not have positive producerCount", source);
        checkArgument(shuffleInfo.getConsumerCount() > 0,
                "%s does not have positive consumerCount", source);
        checkArgument(shuffleInfo.getPollTimeoutSeconds() > 0,
                "%s does not have positive pollTimeoutSeconds", source);
        checkArgument(shuffleInfo.getQueues() != null,
                "%s does not have partition queues", source);
        checkArgument(shuffleInfo.getQueues().size() == shuffleInfo.getNumPartitions(),
                "%s queue count does not match numPartitions", source);

        Set<Integer> partitionIds = new HashSet<>();
        for (ShuffleQueueInfo queue : shuffleInfo.getQueues())
        {
            checkArgument(queue != null, "%s has null queue info", source);
            int partitionId = queue.getPartitionId();
            checkArgument(partitionId >= 0 && partitionId < shuffleInfo.getNumPartitions(),
                    "%s has out-of-range queue partitionId %s", source, partitionId);
            checkArgument(partitionIds.add(partitionId),
                    "%s has duplicate queue partitionId %s", source, partitionId);
            checkArgument(!isNullOrEmpty(queue.getQueueName()) || !isNullOrEmpty(queue.getQueueUrl()),
                    "%s partition %s does not have queueName or queueUrl", source, partitionId);
        }
    }

    private static boolean isNullOrEmpty(String value)
    {
        return value == null || value.trim().isEmpty();
    }

    private static boolean isS3QSShuffle(ShuffleInfo shuffleInfo)
    {
        return shuffleInfo != null && shuffleInfo.getStorageInfo() != null &&
                shuffleInfo.getStorageInfo().getScheme() == Storage.Scheme.s3qs;
    }

    private void checkS3QSTable(PartitionedTableInfo tableInfo, String side)
    {
        checkArgument(tableInfo != null, "%s join table is null", side);
        checkS3QSShuffleInfo(tableInfo.getShuffleInfo(), side + " join table");
    }

    private void validateJoinInputs()
    {
        for (JoinInput joinInput : joinInputs)
        {
            if (joinAlgo == JoinAlgorithm.PARTITIONED)
            {
                PartitionedJoinInput partitionedJoinInput = (PartitionedJoinInput) joinInput;
                checkS3QSTable(partitionedJoinInput.getSmallTable(), "small");
                checkS3QSTable(partitionedJoinInput.getLargeTable(), "large");
            }
            else if (joinAlgo == JoinAlgorithm.PARTITIONED_CHAIN)
            {
                PartitionedChainJoinInput chainJoinInput = (PartitionedChainJoinInput) joinInput;
                checkS3QSTable(chainJoinInput.getSmallTable(), "small");
                checkS3QSTable(chainJoinInput.getLargeTable(), "large");
            }
        }
    }

}
