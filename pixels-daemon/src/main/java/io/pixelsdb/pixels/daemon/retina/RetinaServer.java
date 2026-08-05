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

import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.daemon.heartbeat.HeartbeatWorker;
import io.pixelsdb.pixels.daemon.heartbeat.NodeStatus;
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

    private volatile boolean running = false;
    private final int port;
    private volatile io.grpc.Server rpcServer;

    public RetinaServer(int port)
    {
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        this.port = port;
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
        io.grpc.Server server = this.rpcServer;
        if (server != null)
        {
            try
            {
                server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e)
            {
                log.error("Interrupted when shutdown rpc server", e);
            }
        }
    }

    @Override
    public void run()
    {
        try
        {
            HeartbeatWorker.setCurrentStatus(NodeStatus.INIT);
            RetinaServerImpl service = new RetinaServerImpl();
            service.setReadyListener(() -> publishReady(service));
            io.grpc.Server server = ServerBuilder.forPort(port)
                    .addService(service).build();
            this.rpcServer = server;
            server.start();
            this.running = true;
            publishReady(service);
            server.awaitTermination();
        } catch (IOException e)
        {
            log.error("I/O error when running", e);
        } catch (InterruptedException e)
        {
            log.error("Interrupted when running", e);
        } finally
        {
            this.shutdown();
        }
    }

    private void publishReady(RetinaServerImpl service)
    {
        if (this.running && service.isReady())
        {
            HeartbeatWorker.setCurrentStatus(NodeStatus.READY);
            log.info("Retina service is ready");
        }
    }
}
