/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.invoker.spike;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;

public class S3QSPartitionedChainJoinInvoker extends SpikeInvoker
{
    protected S3QSPartitionedChainJoinInvoker(String functionName)
    {
        super(functionName, WorkerType.PARTITIONED_CHAIN_JOIN_S3QS);
    }

    @Override
    public Output parseOutput(String outputJson)
    {
        return JSON.parseObject(outputJson, JoinOutput.class);
    }
}
