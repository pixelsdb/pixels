/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.invoker.spike;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;

import java.util.concurrent.CompletableFuture;

public class ScanStreamInvoker extends SpikeInvoker
{
    protected ScanStreamInvoker(String functionName)
    {
        super(functionName, WorkerType.SCAN_STREAMING);
    }

    @Override
    public Output parseOutput(String outputJson)
    {
        return JSON.parseObject(outputJson, ScanOutput.class);
    }

    @Override
    public CompletableFuture<Output> invoke(Input input)
    {
        ScanInput scanInput = (ScanInput) input;
        scanInput.setRequiredCpu(scanInput.getTableInfo().getInputSplits().size());
        return super.invoke(scanInput);
    }
}
