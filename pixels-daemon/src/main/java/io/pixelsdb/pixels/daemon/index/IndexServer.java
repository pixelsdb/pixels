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
package io.pixelsdb.pixels.daemon.index;

import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.SecondaryIndex;
import io.pixelsdb.pixels.common.index.SecondaryIndexProvider;
import io.pixelsdb.pixels.common.server.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2025-02-19
 */
public class IndexServer implements Server
{
    private static final Logger log = LogManager.getLogger(IndexServer.class);
    private boolean running = false;
    private final io.grpc.Server rpcServer;
    public IndexServer(int port, SecondaryIndex secondaryIndex, MainIndex mainIndex)
    {
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        this.rpcServer = ServerBuilder.forPort(port)
                .addService(new IndexServiceImpl(secondaryIndex, mainIndex)).build();
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
            log.error("interrupted when shutdown index server", e);
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
