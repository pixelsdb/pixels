package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.WorkerProto;

import java.util.concurrent.CompletableFuture;

public class BroadcastJoinInvoker extends VhiveInvoker {
    protected BroadcastJoinInvoker(String functionName) {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson) {
        return JSON.parseObject(outputJson, JoinOutput.class);
    }

    @Override
    public CompletableFuture<Output> invoke(Input input) {
        ListenableFuture<WorkerProto.WorkerResponse> future = Vhive.Instance().getAsyncClient().broadcastJoin((BroadcastJoinInput) input);
        return genCompletableFuture(future);
    }
}
