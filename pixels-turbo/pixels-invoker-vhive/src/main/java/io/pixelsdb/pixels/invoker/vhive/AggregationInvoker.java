package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.turbo.TurboProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public class AggregationInvoker extends VhiveInvoker {
    private final Logger log = LogManager.getLogger(AggregationInvoker.class);
    protected AggregationInvoker(String functionName) {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson) {
        return JSON.parseObject(outputJson, AggregationOutput.class);
    }

    @Override
    public CompletableFuture<Output> invoke(Input input) {
        log.info(String.format("invoke AggregationInput: %s", JSON.toJSONString(input, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect)));
        ListenableFuture<TurboProto.WorkerResponse> future = Vhive.Instance().getAsyncClient().aggregation((AggregationInput) input);
        return genCompletableFuture(future);
    }
}
