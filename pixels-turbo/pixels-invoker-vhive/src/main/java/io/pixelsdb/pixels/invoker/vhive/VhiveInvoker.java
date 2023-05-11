package io.pixelsdb.pixels.invoker.vhive;

import io.pixelsdb.pixels.worker.common.WorkerProto;
import io.pixelsdb.pixels.common.turbo.Invoker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;

public abstract class VhiveInvoker implements Invoker {
    private static  final Logger logger = LogManager.getLogger(VhiveInvoker.class);
    private final String functionName;
    private final int memoryMB;

    protected VhiveInvoker(String functionName) throws ExecutionException, InterruptedException {
        this.functionName = functionName;
        WorkerProto.GetMemoryResponse response = Vhive.Instance().getAsyncClient().getMemory().get();
        this.memoryMB = (int)response.getMemoryMB();
    }

    @Override
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public int getMemoryMB() {
        return memoryMB;
    }
}
