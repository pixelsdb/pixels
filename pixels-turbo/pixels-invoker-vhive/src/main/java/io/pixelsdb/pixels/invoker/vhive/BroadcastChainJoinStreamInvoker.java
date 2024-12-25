package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.turbo.TurboProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;


public class BroadcastChainJoinStreamInvoker extends VhiveInvoker
{
    private final Logger log = LogManager.getLogger(BroadcastChainJoinStreamInvoker.class);

    protected BroadcastChainJoinStreamInvoker(String functionName) { super(functionName); }

    @Override
    public Output parseOutput(String outputJson) { return JSON.parseObject(outputJson, JoinOutput.class); }

    @Override
    public CompletableFuture<Output> invoke(Input input)
    {
        ListenableFuture<TurboProto.WorkerResponse> future = Vhive.Instance().getAsyncClient().broadcastChainJoinStream((BroadcastChainJoinInput) input);
        return genCompletableFuture(future);
    }
}
