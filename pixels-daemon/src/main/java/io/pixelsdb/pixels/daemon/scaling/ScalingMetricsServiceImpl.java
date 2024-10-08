/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon.scaling;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.turbo.ScalingServiceGrpc;
import io.pixelsdb.pixels.turbo.TurboProto;
import io.pixelsdb.pixels.daemon.scaling.util.PolicyManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingDeque;

public class ScalingMetricsServiceImpl extends ScalingServiceGrpc.ScalingServiceImplBase
{
    private static final Logger log = LogManager.getLogger(ScalingMetricsServiceImpl.class);
    private final BlockingDeque<Integer> metricsQueue;

    public ScalingMetricsServiceImpl()
    {
        metricsQueue = MetricsQueue.queue;
        new Thread(new PolicyManager()).start();
    }

    @Override
    public StreamObserver<TurboProto.CollectMetricsRequest> collectMetrics(StreamObserver<TurboProto.CollectMetricsResponse> responseObserver)
    {
        return new StreamObserver<TurboProto.CollectMetricsRequest>()
        {
            @Override
            public void onNext(TurboProto.CollectMetricsRequest request)
            {
                metricsQueue.add(request.getQueryConcurrency());
                log.info("Collect metrics:" + metricsQueue);
            }

            @Override
            public void onError(Throwable t)
            {
                log.error("Client send metrics error: " + t.getMessage());
            }

            @Override
            public void onCompleted()
            {
                log.info("Client completed sending metrics");
            }
        };
    }

    @Override
    public void checkMetrics(TurboProto.CheckMetricsRequest request, StreamObserver<TurboProto.CheckMetricsResponse> responseObserver)
    {
        super.checkMetrics(request, responseObserver);
    }
}