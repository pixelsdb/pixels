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
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.*;
import io.pixelsdb.pixels.common.utils.ListenableFutureAdapter;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.spike.handler.SpikeServiceProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class SpikeInvoker implements Invoker
{
    private static final Logger logger = LogManager.getLogger(SpikeInvoker.class);

    private final String functionName;
    private final WorkerType workerType;
    private final int memoryMB;

    protected SpikeInvoker(String functionName, WorkerType workerType)
    {
        if (workerType == WorkerType.PARTITION_S3QS ||
                workerType == WorkerType.PARTITIONED_JOIN_S3QS ||
                workerType == WorkerType.PARTITIONED_CHAIN_JOIN_S3QS)
        {
            checkArgument(functionName != null && !functionName.trim().isEmpty(),
                    "Spike S3QS function name is not configured for %s", workerType);
        }
        this.functionName = functionName;
        // TODO: how to set memoryMB?
        this.memoryMB = 0;
        this.workerType = workerType;
        logger.info("function: " + this.functionName + ", worker: " + this.workerType + ", memory size: " + this.memoryMB);
    }

    public CompletableFuture<Output> genCompletableFuture(ListenableFuture<SpikeServiceProto.CallFunctionResponse> listenableFuture)
    {
        CompletableFuture<SpikeServiceProto.CallFunctionResponse> completableFuture = ListenableFutureAdapter.toCompletable(listenableFuture);
        return completableFuture.handle((response, err) ->
        {
            if (response == null)
            {
                throw new CompletionException("failed to execute Spike function " + functionName, err);
            }
            else if (response.getErrorCode() == 0)
            {
                String outputJson = response.getPayload();
                Output output = this.parseOutput(outputJson);
                if (output == null)
                {
                    throw new RuntimeException("failed to parse response payload, payload=" + response.getPayload());
                }
                output.setMemoryMB(this.memoryMB);
                return output;
            }
            else
            {
                throw new RuntimeException("failed to execute the request, function error (" +
                        response.getErrorCode() + ")");
            }
        });
    }

    public CompletableFuture<Output> invoke(Input input)
    {
        SpikeWorkerRequest workerRequest = new SpikeWorkerRequest(this.workerType,
                JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect));
        SpikeAsyncClient client = SpikeAsyncClient.Instance();
        int requiredCpu = input.getRequiredCpu();
        int requiredMemory = input.getRequiredMemory();
        if (input instanceof StageWorkerInput)
        {
            checkArgument(requiredCpu >= 0 && requiredMemory >= 0,
                    "Spike S3QS worker resource request must not be negative");
            requiredCpu = requiredCpu == 0 ? client.getDefaultCpu() : requiredCpu;
            requiredMemory = requiredMemory == 0 ? client.getDefaultMemory() : requiredMemory;
        }
        ListenableFuture<SpikeServiceProto.CallFunctionResponse> future = client.invoke(
                this.functionName,
                JSON.toJSONString(workerRequest, SerializerFeature.DisableCircularReferenceDetect),
                requiredCpu, requiredMemory);
        return genCompletableFuture(future);
    }

    @Override
    public String getFunctionName()
    {
        return functionName;
    }

    @Override
    public int getMemoryMB()
    {
        return memoryMB;
    }
}
