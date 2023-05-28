package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public class BroadcastJoinInvoker extends VhiveInvoker {
    private final Logger log = LogManager.getLogger(BroadcastJoinInvoker.class);
    protected BroadcastJoinInvoker(String functionName) {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson) {
        return JSON.parseObject(outputJson, JoinOutput.class);
    }

    @Override
    public CompletableFuture<Output> invoke(Input input) {
        log.info(String.format("invoke BroadcastJoinInput: %s", JSON.toJSONString(input, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect)));
        ListenableFuture<WorkerProto.WorkerResponse> future = Vhive.Instance().getAsyncClient().broadcastJoin((BroadcastJoinInput) input);
        return genCompletableFuture(future);
    }
}
