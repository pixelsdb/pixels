package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.turbo.TurboProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public class PartitionChainJoinInvoker extends VhiveInvoker {
    private final Logger log = LogManager.getLogger(PartitionChainJoinInvoker.class);
    protected PartitionChainJoinInvoker(String functionName) {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson) {
        return JSON.parseObject(outputJson, JoinOutput.class);
    }

    @Override
    public CompletableFuture<Output> invoke(Input input) {
//        log.info(String.format("invoke PartitionedChainJoinInput: %s", JSON.toJSONString(input, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect)));
        ListenableFuture<TurboProto.WorkerResponse> future = Vhive.Instance().getAsyncClient().partitionChainJoin((PartitionedChainJoinInput) input);
        return genCompletableFuture(future);
    }
}
