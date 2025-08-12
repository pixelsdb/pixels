/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.daemon.transaction;

import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.common.server.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2022-02-20
 */
public class TransServer implements Server
{
    private static final Logger log = LogManager.getLogger(TransServer.class);

    private boolean running = false;
    private final io.grpc.Server rpcServer;

    public TransServer(int port)
    {
        assert (port > 0 && port <= 65535);
        this.rpcServer = ServerBuilder.forPort(port)
                .addService(new TransServiceImpl()).build();
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
            log.error("Interrupted when shutdown transaction server.", e);
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
}
