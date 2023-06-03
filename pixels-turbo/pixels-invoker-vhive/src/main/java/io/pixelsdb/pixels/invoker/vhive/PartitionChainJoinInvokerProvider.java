package io.pixelsdb.pixels.invoker.vhive;

import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

public class PartitionChainJoinInvokerProvider implements InvokerProvider
{
    private static final ConfigFactory config = ConfigFactory.Instance();

    @Override
    public Invoker createInvoker()
    {
        String partitionChainJoinWorker = config.getProperty("partitioned.chain.join.worker.name");
        return new PartitionChainJoinInvoker(partitionChainJoinWorker);
    }

    @Override
    public WorkerType workerType()
    {
        return WorkerType.PARTITIONED_CHAIN_JOIN;
    }

    @Override
    public boolean compatibleWith(FunctionService functionService)
    {
        return functionService.equals(FunctionService.vhive);
    }
}
