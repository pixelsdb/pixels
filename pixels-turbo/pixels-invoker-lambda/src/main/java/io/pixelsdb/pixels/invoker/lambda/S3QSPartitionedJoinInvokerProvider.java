/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.invoker.lambda;

import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import static io.pixelsdb.pixels.common.turbo.FunctionService.lambda;
import static io.pixelsdb.pixels.common.turbo.WorkerType.PARTITIONED_JOIN_S3QS;

public class S3QSPartitionedJoinInvokerProvider implements InvokerProvider
{
    private static final ConfigFactory config = ConfigFactory.Instance();

    @Override
    public Invoker createInvoker()
    {
        return new S3QSPartitionedJoinInvoker(config.getProperty("s3qs.partitioned.join.worker.name"));
    }

    @Override
    public WorkerType workerType()
    {
        return PARTITIONED_JOIN_S3QS;
    }

    @Override
    public boolean compatibleWith(FunctionService functionService)
    {
        return functionService.equals(lambda);
    }
}
