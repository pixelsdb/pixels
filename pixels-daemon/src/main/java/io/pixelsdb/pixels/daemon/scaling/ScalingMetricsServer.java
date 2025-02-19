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

import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.common.server.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class ScalingMetricsServer implements Server
{
    private static final Logger log = LogManager.getLogger(ScalingMetricsServer.class);
    private boolean running = false;
    private final io.grpc.Server rpcServer;
    private final int port;

    public ScalingMetricsServer(int port)
    {
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        this.port = port;
        this.rpcServer = ServerBuilder.forPort(port)
                .addService(new ScalingMetricsServiceImpl()).build();
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
            log.error("interrupted when shutdown scaling metrics server", e);
        }
    }

    @Override
    public void run()
    {
        try
        {
            this.rpcServer.start();
            log.info("Server started, listening on " + port);
            this.running = true;
            log.info("ScalingMetricsServer is running");
            this.rpcServer.awaitTermination();
        } catch (IOException e)
        {
            log.error("I/O error when running", e);
        } catch (InterruptedException e)
        {
            log.error("interrupted when running", e);
        } finally
        {
            this.shutdown();
        }
    }
}
