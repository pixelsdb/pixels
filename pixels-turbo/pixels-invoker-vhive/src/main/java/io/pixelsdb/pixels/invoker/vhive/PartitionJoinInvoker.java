package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.turbo.TurboProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public class PartitionJoinInvoker extends VhiveInvoker
{
    private final Logger log = LogManager.getLogger(PartitionJoinInvoker.class);

    protected PartitionJoinInvoker(String functionName)
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
//        log.info(String.format("invoke PartitionedJoinInput: %s", JSON.toJSONString(input, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect)));
        ListenableFuture<TurboProto.WorkerResponse> future = Vhive.Instance().getAsyncClient().partitionJoin((PartitionedJoinInput) input);
        return genCompletableFuture(future);
    }
}
