package io.pixelsdb.pixels.invoker.vhive;

import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.invoker.vhive.utils.ListenableFutureAdapter;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public abstract class VhiveInvoker implements Invoker {
    private static  final Logger logger = LogManager.getLogger(VhiveInvoker.class);
    private final String functionName;
    private final int memoryMB;

    protected VhiveInvoker(String functionName) {
        this.functionName = functionName;
        int memoryMB = 0;
        try {
            WorkerProto.GetMemoryResponse response = Vhive.Instance().getAsyncClient().getMemory().get();
            memoryMB = (int)response.getMemoryMB();
        } catch (Exception e) {
            logger.warn("failed to get memory: " + e);
        }
        this.memoryMB = memoryMB;
    }

    @Override
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public int getMemoryMB() {
        return memoryMB;
    }

    public CompletableFuture<Output> genCompletableFuture(ListenableFuture<WorkerProto.WorkerResponse> listenableFuture) {
        CompletableFuture<WorkerProto.WorkerResponse> completableFuture = ListenableFutureAdapter.toCompletable(listenableFuture);
        return completableFuture.handle((response, err) -> {
            if (err == null) {
                String outputJson = response.getJson();
                Output output = this.parseOutput(outputJson);
                if (output != null) {
                    output.setMemoryMB(this.memoryMB);
                    return output;
                } else {
                    throw new RuntimeException("failed to parse response payload, JSON = " +
                            response.getJson());
                }
            } else {
                throw new RuntimeException("failed to execute the request, function: " +
                        this.getFunctionName() +
                        " with error: " +
                        err);
            }
        });
    }
}
