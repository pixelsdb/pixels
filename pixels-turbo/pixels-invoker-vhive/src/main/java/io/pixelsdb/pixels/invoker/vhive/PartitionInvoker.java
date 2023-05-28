package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public class PartitionInvoker extends VhiveInvoker {
    private final Logger log = LogManager.getLogger(PartitionInvoker.class);
    protected PartitionInvoker(String functionName) {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson) {
        return JSON.parseObject(outputJson, PartitionOutput.class);
    }

    @Override
    public CompletableFuture<Output> invoke(Input input) {
        log.info(String.format("invoke PartitionInput: %s", JSON.toJSONString(input, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect)));
        ListenableFuture<WorkerProto.WorkerResponse> future = Vhive.Instance().getAsyncClient().partition((PartitionInput) input);
        return genCompletableFuture(future);
    }
}
