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
package io.pixelsdb.pixels.planner.plan.physical;

import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinator;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinatorFactory;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2023-09-19
 */
public class PartitionedJoinStreamOperator extends PartitionedJoinOperator
{
    private static final Logger logger = LogManager.getLogger(PartitionedJoinStreamOperator.class);
    private static final CompletableFuture<Void> Completed = CompletableFuture.completedFuture(null);

    public PartitionedJoinStreamOperator(String name, List<PartitionInput> smallPartitionInputs,
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
            // First, bootstrap the join workers.
            joinOutputs = new CompletableFuture[joinInputs.size()];
            for (int i = 0; i < joinInputs.size(); ++i)
            {
                JoinInput joinInput = joinInputs.get(i);
                joinInput.setSmallPartitionWorkerNum(smallPartitionInputs.size());  // XXX: could be 0
                joinInput.setLargePartitionWorkerNum(largePartitionInputs.size());
                if (joinAlgo == JoinAlgorithm.PARTITIONED)
                {
                    joinOutputs[i] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITIONED_JOIN_STREAMING).invoke(joinInput);
                }
                // Not yet implemented
//                else if (joinAlgo == JoinAlgorithm.PARTITIONED_CHAIN)
//                {
//                    joinOutputs[i] = InvokerFactory.Instance()
//                            .getInvoker(WorkerType.PARTITIONED_CHAIN_JOIN_STREAMING).invoke(joinInput);
//                }
                else
                {
                    throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is unsupported");
                }
            }

            // Then, wait for the readiness of the join workers, and then bootstrap the partition workers
            //  because the partition workers will write to the ports of the join workers.
            long transId = joinInputs.get(0).getTransId();
            PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(transId);
            planCoordinator.getStageCoordinator(joinInputs.get(0).getStageId()).waitForAllWorkersReady();

            CompletableFuture<CompletableFuture<? extends Output>[]> smallChildFuture = null;
            CompletableFuture<CompletableFuture<? extends Output>[]> largeChildFuture = null;
            if (smallChild != null && largeChild != null)
            {
                // both children exist, we should execute both children.
                checkArgument(smallPartitionInputs.isEmpty(), "smallPartitionInputs is not empty");
                checkArgument(largePartitionInputs.isEmpty(), "largePartitionInputs is not empty");
                smallChildFuture = smallChild.execute();
                largeChildFuture = largeChild.execute();
            }
            else if (smallChild != null)
            {
                // only small child exists, we should invoke the large table partitioning and execute the small child.
                checkArgument(smallPartitionInputs.isEmpty(), "smallPartitionInputs is not empty");
                checkArgument(!largePartitionInputs.isEmpty(), "largePartitionInputs is empty");
                smallChildFuture = smallChild.execute();
                largePartitionOutputs = new CompletableFuture[largePartitionInputs.size()];
                int i = 0;
                for (PartitionInput partitionInput : largePartitionInputs)
                {
                    largePartitionOutputs[i++] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITION_STREAMING).invoke((partitionInput));
                }

                logger.debug("invoke large partition of " + this.getName());
            }
            else if (largeChild != null)
            {
                // only large child exists, we should invoke the small table partitioning.
                checkArgument(!smallPartitionInputs.isEmpty(), "smallPartitionInputs is empty");
                checkArgument(largePartitionInputs.isEmpty(), "largePartitionInputs is not empty");
                smallPartitionOutputs = new CompletableFuture[smallPartitionInputs.size()];
                int i = 0;
                for (PartitionInput partitionInput : smallPartitionInputs)
                {
                    smallPartitionOutputs[i++] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITION_STREAMING).invoke((partitionInput));
                }

                logger.debug("invoke small partition of " + this.getName());

                largeChildFuture = largeChild.execute();
            }
            else
            {
                // no children exist, partition both tables and wait for the small table partitioning.
                checkArgument(!smallPartitionInputs.isEmpty(), "smallPartitionInputs is empty");
                checkArgument(!largePartitionInputs.isEmpty(), "largePartitionInputs is empty");
                smallPartitionOutputs = new CompletableFuture[smallPartitionInputs.size()];
                int i = 0;
                for (PartitionInput partitionInput : smallPartitionInputs)
                {
                    smallPartitionOutputs[i++] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITION_STREAMING).invoke((partitionInput));
                }

                logger.debug("invoke small partition of " + this.getName());

                largePartitionOutputs = new CompletableFuture[largePartitionInputs.size()];
                i = 0;
                for (PartitionInput partitionInput : largePartitionInputs)
                {
                    largePartitionOutputs[i++] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITION_STREAMING).invoke((partitionInput));
                }

                logger.debug("invoke large partition of " + this.getName());
            }

            // todo: Finally, wait for the readiness of the partition operators

            return joinOutputs;
        });
    }

    @Override
    public CompletableFuture<Void> executePrev()
    {
        return Completed;
    }
}
