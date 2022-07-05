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

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.executor.lambda.input.AggregationInput;
import io.pixelsdb.pixels.executor.lambda.input.ScanInput;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 05/07/2022
 */
public class AggregationOperator implements Operator
{
    private final AggregationInput aggregationInput;
    private final List<ScanInput> scanInputs;
    private Operator child = null;
    private CompletableFuture<?>[] scanOutputs = null;
    private CompletableFuture<?> aggregateOutput = null;

    public AggregationOperator(AggregationInput aggregationInput, List<ScanInput> scanInputs)
    {
        this.aggregationInput = requireNonNull(aggregationInput, "aggregateInput is null");
        if (scanInputs == null || scanInputs.isEmpty())
        {
            this.scanInputs = ImmutableList.of();
        }
        else
        {
            this.scanInputs = ImmutableList.copyOf(scanInputs);
        }
    }

    public AggregationInput getAggregationInput()
    {
        return aggregationInput;
    }

    public List<ScanInput> getScanInputs()
    {
        return scanInputs;
    }

    public void setChild(Operator child)
    {
        if (child == null)
        {
            checkArgument(!this.scanInputs.isEmpty(),
                    "scanInputs must be non-empty if child is set to null");
            this.child = null;
        }
        else
        {
            checkArgument(this.scanInputs.isEmpty(),
                    "scanInputs must be empty if child is set to non-null");
            this.child = child;
        }
    }

    @Override
    public CompletableFuture<?>[] execute()
    {
        // TODO: implement.
        return new CompletableFuture[0];
    }

    @Override
    public CompletableFuture<?>[] executePrev()
    {
        // TODO: implement.
        return new CompletableFuture[0];
    }

    @Override
    public OutputCollection collectOutputs() throws ExecutionException, InterruptedException
    {
        // TODO: implement.
        return null;
    }
}
