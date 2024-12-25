package io.pixelsdb.pixels.invoker.vhive;

import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

public class BroadcastChainJoinStreamInvokerProvider implements InvokerProvider
{
    private static final ConfigFactory config = ConfigFactory.Instance();

    @Override
    public Invoker createInvoker()
    {
        String broadcastChainJoinWorker = config.getProperty("broadcast.chain.join.worker.name");
        return new BroadcastChainJoinStreamInvoker(broadcastChainJoinWorker);
    }

    @Override
    public WorkerType workerType() { return WorkerType.BROADCAST_CHAIN_JOIN_STREAMING; }

    @Override
    public boolean compatibleWith(FunctionService functionService)
    {
        return functionService.equals(FunctionService.vhive);
    }
}