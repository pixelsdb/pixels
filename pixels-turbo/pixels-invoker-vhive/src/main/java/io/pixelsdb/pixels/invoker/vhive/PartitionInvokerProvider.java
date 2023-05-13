package io.pixelsdb.pixels.invoker.vhive;

import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

public class PartitionInvokerProvider implements InvokerProvider {
    private static final ConfigFactory config = ConfigFactory.Instance();

    @Override
    public Invoker createInvoker() {
        String partitionWorker = config.getProperty("partition.worker.name");
        return new PartitionInvoker(partitionWorker);
    }

    @Override
    public WorkerType workerType() {
        return WorkerType.PARTITION;
    }

    @Override
    public boolean compatibleWith(FunctionService functionService) {
        return functionService.equals(FunctionService.vhive);
    }
}
