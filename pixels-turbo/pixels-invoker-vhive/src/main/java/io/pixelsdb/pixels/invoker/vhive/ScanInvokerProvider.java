package io.pixelsdb.pixels.invoker.vhive;

import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

public class ScanInvokerProvider implements InvokerProvider {
    private static final ConfigFactory config = ConfigFactory.Instance();

    @Override
    public Invoker createInvoker() {
        String scanWorker = config.getProperty("scan.worker.name");
        return new ScanInvoker(scanWorker);
    }

    @Override
    public WorkerType workerType() {
        return WorkerType.SCAN;
    }

    @Override
    public boolean compatibleWith(FunctionService functionService) {
        return functionService.equals(FunctionService.vhive);
    }
}
