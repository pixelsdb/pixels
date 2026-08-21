/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public License
 * along with Pixels.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.OutputCollection;
import io.pixelsdb.pixels.planner.plan.physical.ScanOperator.ScanOutputCollection;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestCoordinatedPlanExecution
{
    private static final AtomicLong NextTransId = new AtomicLong(12770000L);

    @Test
    public void coordinatorRemainsRegisteredUntilOutputsAreCollected() throws Exception
    {
        long transId = NextTransId.getAndIncrement();
        PlanCoordinatorFactory factory = PlanCoordinatorFactory.Instance();
        RecordingOperator operator = new RecordingOperator(transId);
        CoordinatedPlanExecution execution = factory.createPlanExecution(transId, operator,
                new CoordinatorEndpoint("coordinator.internal", 19000));

        assertSame(execution.getPlanCoordinator(), factory.getPlanCoordinator(transId));
        assertSame(execution.getPlanCoordinator(), operator.initializedCoordinator);
        assertEquals("coordinator.internal",
                execution.getPlanCoordinator().getCoordinatorEndpoint().getHost());

        execution.execute().get();
        assertTrue(operator.coordinatorWasRegisteredDuringExecute);
        assertSame(execution.getPlanCoordinator(), factory.getPlanCoordinator(transId));

        OutputCollection outputs = execution.collectOutputs();
        assertSame(operator.outputs, outputs);
        assertNull(factory.getPlanCoordinator(transId));
    }

    @Test
    public void explicitCloseCleansUpExecutionThatDoesNotCollectOutputs()
    {
        long transId = NextTransId.getAndIncrement();
        PlanCoordinatorFactory factory = PlanCoordinatorFactory.Instance();
        CoordinatedPlanExecution execution = factory.createPlanExecution(transId,
                new RecordingOperator(transId), new CoordinatorEndpoint("localhost", 18894));

        execution.close();
        execution.close();

        assertNull(factory.getPlanCoordinator(transId));
    }

    @Test
    public void outputCollectionWaitsForFinalStageStartup() throws Exception
    {
        long transId = NextTransId.getAndIncrement();
        PlanCoordinatorFactory factory = PlanCoordinatorFactory.Instance();
        RecordingOperator operator = new RecordingOperator(transId);
        operator.executionFuture = new CompletableFuture<>();
        CoordinatedPlanExecution execution = factory.createPlanExecution(transId, operator,
                new CoordinatorEndpoint("localhost", 18894));
        execution.execute();

        CompletableFuture<OutputCollection> collected =
                CompletableFuture.supplyAsync(() ->
                {
                    try
                    {
                        return execution.collectOutputs();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                });
        assertTrue(!collected.isDone());

        operator.executionFuture.complete(new CompletableFuture[0]);
        assertSame(operator.outputs, collected.get());
        assertNull(factory.getPlanCoordinator(transId));
    }

    @Test
    public void shuffleResourcesArePreparedBeforeExecutionAndCleanedOnce() throws Exception
    {
        long transId = NextTransId.getAndIncrement();
        PlanCoordinatorFactory factory = PlanCoordinatorFactory.Instance();
        RecordingOperator operator = new RecordingOperator(transId);
        operator.shuffleInfo = shuffleInfo("shuffle-lifecycle");
        RecordingShuffleResourceLifecycle lifecycle = new RecordingShuffleResourceLifecycle();
        operator.lifecycle = lifecycle;
        CoordinatedPlanExecution execution = factory.createPlanExecution(transId, operator,
                new CoordinatorEndpoint("localhost", 18894), lifecycle);

        execution.execute().get();
        assertTrue(operator.resourcesWerePreparedDuringExecute);
        assertEquals(1, lifecycle.preparedShuffleCount);

        execution.collectOutputs();
        execution.close();
        assertEquals(1, lifecycle.cleanupCount);
        assertNull(factory.getPlanCoordinator(transId));
    }

    @Test(expected = IllegalStateException.class)
    public void duplicateTransactionCanNotReplaceActiveCoordinator()
    {
        long transId = NextTransId.getAndIncrement();
        PlanCoordinatorFactory factory = PlanCoordinatorFactory.Instance();
        CoordinatedPlanExecution execution = factory.createPlanExecution(transId,
                new RecordingOperator(transId), new CoordinatorEndpoint("localhost", 18894));
        try
        {
            factory.createPlanExecution(transId, new RecordingOperator(transId),
                    new CoordinatorEndpoint("localhost", 18894));
        }
        finally
        {
            execution.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void endpointRejectsEmptyHost()
    {
        new CoordinatorEndpoint(" ", 18894);
    }

    @Test(expected = IllegalArgumentException.class)
    public void endpointRejectsInvalidPort()
    {
        new CoordinatorEndpoint("localhost", 0);
    }

    private static ShuffleInfo shuffleInfo(String shuffleId)
    {
        return new ShuffleInfo(shuffleId,
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "bucket/" + shuffleId + "/", 1, 1, 1, 1,
                Collections.singletonList(new ShuffleQueueInfo(0, shuffleId + "-p0", null)));
    }

    private static class RecordingOperator extends Operator
    {
        private final long transId;
        private final OutputCollection outputs = new ScanOutputCollection();
        private PlanCoordinator initializedCoordinator;
        private boolean coordinatorWasRegisteredDuringExecute;
        private boolean resourcesWerePreparedDuringExecute;
        private ShuffleInfo shuffleInfo;
        private RecordingShuffleResourceLifecycle lifecycle;
        private CompletableFuture<CompletableFuture<? extends Output>[]> executionFuture =
                CompletableFuture.completedFuture(new CompletableFuture[0]);

        private RecordingOperator(long transId)
        {
            super("recording-plan");
            this.transId = transId;
        }

        @Override
        public void initPlanCoordinator(PlanCoordinator planCoordinator, int parentStageId,
                                        boolean wideDependOnParent)
        {
            this.initializedCoordinator = planCoordinator;
            if (shuffleInfo != null)
            {
                planCoordinator.addShuffleInfo(shuffleInfo);
            }
        }

        @Override
        public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
        {
            coordinatorWasRegisteredDuringExecute =
                    PlanCoordinatorFactory.Instance().getPlanCoordinator(transId) == initializedCoordinator;
            resourcesWerePreparedDuringExecute = lifecycle != null && lifecycle.prepared;
            return executionFuture;
        }

        @Override
        public CompletableFuture<Void> executePrev()
        {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public OutputCollection collectOutputs()
        {
            return outputs;
        }
    }

    private static class RecordingShuffleResourceLifecycle implements ShuffleResourceLifecycle
    {
        private boolean prepared;
        private int preparedShuffleCount;
        private int cleanupCount;

        @Override
        public void prepare(Collection<ShuffleInfo> shuffleInfos)
        {
            prepared = true;
            preparedShuffleCount = shuffleInfos.size();
        }

        @Override
        public void cleanup() throws IOException
        {
            ++cleanupCount;
        }
    }
}
