/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.daemon.metadata;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.StartupCheck;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Waits until the Metadata gRPC health service reports SERVING.
 *
 * @author PixelsDB
 */
public class MetadataReadyCheck implements StartupCheck
{
    private static final Logger log = LogManager.getLogger(MetadataReadyCheck.class);
    private static final long HEALTH_CHECK_TIMEOUT_MS = 1_000L;
    private static final long RETRY_INTERVAL_MS = 1_000L;

    @Override
    public String getDescription()
    {
        return "Metadata gRPC health service to report SERVING";
    }

    @Override
    public void awaitReady(long deadlineNanos) throws InterruptedException
    {
        ConfigFactory config = ConfigFactory.Instance();
        String host = config.getProperty("metadata.server.host");
        int port = Integer.parseInt(config.getProperty("metadata.server.port"));

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try
        {
            HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(channel);
            log.info("Waiting for Metadata server {}:{} to report SERVING", host, port);
            while (true)
            {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0)
                {
                    throw new IllegalStateException(
                            "Timed out waiting for Metadata server " + host + ":" + port
                                    + " to report SERVING");
                }
                try
                {
                    HealthCheckResponse response = stub
                            .withDeadlineAfter(
                                    Math.min(
                                            TimeUnit.MILLISECONDS.toNanos(HEALTH_CHECK_TIMEOUT_MS),
                                            remainingNanos),
                                    TimeUnit.NANOSECONDS)
                            .check(HealthCheckRequest.newBuilder().setService("metadata").build());
                    if (response.getStatus() == HealthCheckResponse.ServingStatus.SERVING)
                    {
                        log.info("Metadata server {}:{} is ready", host, port);
                        return;
                    }
                }
                catch (StatusRuntimeException e)
                {
                    if (Thread.currentThread().isInterrupted())
                    {
                        throw new InterruptedException("Interrupted while checking Metadata readiness");
                    }
                    log.debug("Metadata health check failed: {}", e.getStatus());
                }
                remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0)
                {
                    throw new IllegalStateException(
                            "Timed out waiting for Metadata server " + host + ":" + port
                                    + " to report SERVING");
                }
                TimeUnit.NANOSECONDS.sleep(Math.min(
                        TimeUnit.MILLISECONDS.toNanos(RETRY_INTERVAL_MS), remainingNanos));
            }
        }
        finally
        {
            channel.shutdownNow();
        }
    }
}
