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

import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestBatchStageFailures
{
    @Test(timeout = 5000)
    public void childStartupFailureReachesCaller() throws Exception
    {
        RuntimeException original = new RuntimeException("child startup failed");
        CompletableFuture<CompletableFuture<? extends Output>[]> startup = new CompletableFuture<>();
        startup.completeExceptionally(original);
        for (Operator operator : parents(child(startup)))
        {
            assertSame(original, rootFailure(operator.executePrev()));
        }
    }

    @Test(timeout = 5000)
    public void failedAndCancelledWorkersCannotLeaveParentPending() throws Exception
    {
        for (boolean cancel : Arrays.asList(false, true))
        {
            CompletableFuture<Output> worker = new CompletableFuture<>();
            if (cancel) { worker.cancel(false); }
            else { worker.completeExceptionally(new RuntimeException("remote OOM")); }
            for (Operator operator : parents(child(CompletableFuture.completedFuture(
                    new CompletableFuture[]{worker}))))
            {
                assertTrue(rootFailure(operator.executePrev()) instanceof IllegalArgumentException);
            }
        }
    }

    @Test(timeout = 5000)
    public void synchronousStartupFailureReachesCaller() throws Exception
    {
        JoinOperator child = mock(JoinOperator.class);
        RuntimeException original = new RuntimeException("invoker unavailable");
        when(child.execute()).thenThrow(original);
        for (Operator operator : parents(child))
        {
            assertSame(original, rootFailure(operator.executePrev()));
        }
    }

    @Test(timeout = 5000)
    public void successfulChildrenStillCompleteNormally() throws Exception
    {
        for (Operator operator : parents(child(CompletableFuture.completedFuture(new CompletableFuture[0]))))
        {
            operator.executePrev().get(1, TimeUnit.SECONDS);
        }
    }

    private static JoinOperator child(CompletableFuture<CompletableFuture<? extends Output>[]> startup)
    {
        JoinOperator child = mock(JoinOperator.class);
        when(child.execute()).thenReturn(startup);
        return child;
    }

    private static List<Operator> parents(JoinOperator child)
    {
        List<JoinInput> inputs = Collections.emptyList();
        SingleStageJoinBatchOperator broadcast = new SingleStageJoinBatchOperator(
                "broadcast", true, inputs, JoinAlgorithm.BROADCAST);
        PartitionedJoinBatchOperator partitioned = new PartitionedJoinBatchOperator(
                "partitioned", null, null, inputs, JoinAlgorithm.PARTITIONED);
        SortedJoinBatchOperator sorted = new SortedJoinBatchOperator(
                "sorted", null, null, inputs, JoinAlgorithm.SORTED);
        for (JoinOperator parent : Arrays.asList(broadcast, partitioned, sorted))
        {
            parent.setSmallChild(child);
            parent.setLargeChild(child);
        }
        AggregationBatchOperator aggregation = new AggregationBatchOperator("aggregation",
                Collections.singletonList(new AggregationInput()), null);
        aggregation.setChild(child);
        return Arrays.asList(broadcast, partitioned, sorted, aggregation);
    }

    private static Throwable rootFailure(CompletableFuture<?> result) throws Exception
    {
        try { result.get(1, TimeUnit.SECONDS); fail("expected failure"); return null; }
        catch (ExecutionException failure)
        {
            Throwable cause = failure;
            while (cause.getCause() != null) { cause = cause.getCause(); }
            return cause;
        }
    }
}
