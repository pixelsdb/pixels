/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.worker.vhive;

import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.worker.vhive.utils.Server;
import io.pixelsdb.pixels.worker.vhive.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class WorkerServer implements Server
{
    private static final Logger log = LogManager.getLogger(WorkerServer.class);
    private final io.grpc.Server rpcServer;
    private boolean running = false;

    public WorkerServer(int port)
    {
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        this.rpcServer = ServerBuilder
                .forPort(port)
                .addService(new WorkerServiceImpl())
                .executor(Executors.newSingleThreadExecutor(Executors.defaultThreadFactory()))
                .build();
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
            log.info("rpc server is trying to shutdown");
            FileAppender appender = LoggerContext.getContext().getConfiguration().getAppender("log");
            String logFilename = appender.getFileName();
            Utils.upload(logFilename, logFilename);
            this.rpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            log.info("rpc server close successfully");
        } catch (InterruptedException e)
        {
            log.error("Interrupted when shutdown rpc server.", e);
        } catch (IOException e)
        {
            log.error("Append log failed when shutdown rpc server.", e);
        }
    }

    @Override
    public void run()
    {
        try
        {
            this.rpcServer.start();
            this.running = true;
            log.info("rpc server run successfully");
            this.rpcServer.awaitTermination();
        } catch (IOException e)
        {
            log.error("I/O error when running.", e);
        } catch (InterruptedException e)
        {
            log.error("Interrupted when running.", e);
        }
    }
}
