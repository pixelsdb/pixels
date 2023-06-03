package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.turbo.TurboProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public class ScanInvoker extends VhiveInvoker
{
    private final Logger log = LogManager.getLogger(ScanInvoker.class);

    protected ScanInvoker(String functionName)
    {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson)
    {
        return JSON.parseObject(outputJson, ScanOutput.class);
    }

    @Override
    public CompletableFuture<Output> invoke(Input input)
    {
//        log.info(String.format("invoke ScanInput: %s", JSON.toJSONString(input, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect)));
        ListenableFuture<TurboProto.WorkerResponse> future = Vhive.Instance().getAsyncClient().scan((ScanInput) input);
        return genCompletableFuture(future);
    }
}
