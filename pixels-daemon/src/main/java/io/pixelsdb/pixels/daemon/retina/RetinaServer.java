/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.daemon.retina;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @create 2024-12-20
 * @author gengdy
 */
public class RetinaServer implements Server
{
    private static final Logger log = LogManager.getLogger(RetinaServer.class);

    private boolean running = false;
    private final io.grpc.Server rpcServer;

    public RetinaServer(int port)
    {
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        // Issue #935: ensure metadata server has been already started
        waitForMetadataServer();
        this.rpcServer = ServerBuilder.forPort(port)
                .addService(new RetinaServerImpl()).build();
    }

    @Override
    public boolean isRunning()
    {
        return this.running;
    }

    @Override
    public void shutdown()
    {
        this.running = false;
        try
        {
            this.rpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e)
        {
            log.error("interrupted when shutdown rpc server", e);
        }
    }

    @Override
    public void run()
    {
        try
        {
            this.rpcServer.start();
            this.running = true;
            this.rpcServer.awaitTermination();
        } catch (IOException e)
        {
            log.error("I/O error when running.", e);
        } catch (InterruptedException e)
        {
            log.error("Interrupted when running.", e);
        } finally
        {
            this.shutdown();
        }
    }

    private void waitForMetadataServer()
    {
        ConfigFactory config = ConfigFactory.Instance();
        String metadataHost = config.getProperty("metadata.server.host");
        int metadataPort = Integer.parseInt(config.getProperty("metadata.server.port"));
        ManagedChannel channel = ManagedChannelBuilder.forAddress(metadataHost, metadataPort)
                .usePlaintext().build();

        HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(channel);
        int retry = 0;
        int maxRetry = 30;

        while (retry < maxRetry)
        {
            try
            {
                HealthCheckResponse response = stub.check(HealthCheckRequest.newBuilder().setService("metadata").build());
                if (response.getStatus() == HealthCheckResponse.ServingStatus.SERVING)
                {
                    log.info("metadata server if ready.");
                    channel.shutdown();
                    return;
                }
            } catch (StatusRuntimeException e)
            {
                log.info("metadata health check failed, sleep one second and retry ...");
            }

            retry++;
            try
            {
                Thread.sleep(1000);
            } catch (InterruptedException e)
            {
                throw new RuntimeException("failed to sleep for retry to check metadata server status", e);
            }
        }
        channel.shutdown();
        throw new RuntimeException("timeout waiting for metadata server to be ready");
    }
}
