/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 */
package io.pixelsdb.pixels.worker.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.S3QSStageWorkerRunner;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class S3QSPartitionedJoinWorker implements RequestHandler<StageWorkerInput, JoinOutput>
{
    private static final Logger logger = LogManager.getLogger(S3QSPartitionedJoinWorker.class);
    private final WorkerMetrics workerMetrics = new WorkerMetrics();

    @Override
    public JoinOutput handleRequest(StageWorkerInput event, Context context)
    {
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, context.getAwsRequestId());
        return new S3QSStageWorkerRunner(workerContext).runPartitionedJoin(event);
    }
}
