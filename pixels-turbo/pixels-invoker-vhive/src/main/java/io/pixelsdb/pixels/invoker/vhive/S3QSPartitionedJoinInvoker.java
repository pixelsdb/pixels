/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.turbo.TurboProto;

import java.util.concurrent.CompletableFuture;

public class S3QSPartitionedJoinInvoker extends VhiveInvoker
{
    protected S3QSPartitionedJoinInvoker(String functionName)
    {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson)
    {
        return JSON.parseObject(outputJson, JoinOutput.class);
    }

    @Override
    public CompletableFuture<Output> invoke(Input input)
    {
        ListenableFuture<TurboProto.vHiveWorkerResponse> future =
                Vhive.Instance().getAsyncClient().partitionJoinS3QS((StageWorkerInput) input);
        return genCompletableFuture(future);
    }
}
