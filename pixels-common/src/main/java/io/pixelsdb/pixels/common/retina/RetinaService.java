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
package io.pixelsdb.pixels.common.retina;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.server.HostAddress;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class RetinaService
{
    private static final Logger logger = LogManager.getLogger(RetinaService.class);
    private static final RetinaService defaultInstance;
    private static final Map<HostAddress, RetinaService> otherInstances = new HashMap<>();

    static
    {
        String retinaHost = ConfigFactory.Instance().getProperty("retina.server.host");
        int retinaPort = Integer.parseInt(ConfigFactory.Instance().getProperty("retina.server.port"));
        defaultInstance = new RetinaService(retinaHost, retinaPort);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try
                {
                    defaultInstance.shutdown();
                    for (RetinaService otherRetinaService : otherInstances.values())
                    {
                        otherRetinaService.shutdown();
                    }
                    otherInstances.clear();
                } catch (InterruptedException e)
                {
                    logger.error("failed to shut down retina service", e);
                }
            }
        }));
    }

    /**
     * Get the default retina service instance connecting to the retina host:port configured in
     * PIXELS_HOME/pixels.properties. This default instance whill be automatically shut down when the process
     * is terminating, no need to call {@link #shutdown()} (although it is idempotent) manually.
     * @return
     */
    public static RetinaService Instance() { return defaultInstance; }

    /**
     * This method should only be used to connect to a retina server that is not configured through
     * PIXELS_HOME/pixels.properties. <b>No need</b> to manually shut down the returned retina service.
     * @param host the host name of the retina server
     * @param port the port of the retina server
     * @return the created retina service instance
     */
    public static RetinaService CreateInstance(String host, int port)
    {
        HostAddress address = HostAddress.fromParts(host, port);
        RetinaService retinaService = otherInstances.get(address);
        if (retinaService != null)
        {
            return retinaService;
        }
        retinaService = new RetinaService(host, port);
        otherInstances.put(address, retinaService);
        return retinaService;
    }
    
    private final ManagedChannel channel;
    private final RetinaWorkerServiceGrpc.RetinaWorkerServiceBlockingStub stub;
    private boolean isShutdown;

    private RetinaService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = RetinaWorkerServiceGrpc.newBlockingStub(this.channel);
        this.isShutdown = false;
    }

    private synchronized void shutdown() throws InterruptedException
    {
        if (!this.isShutdown)
        {
            // Wait for at most 5 seconds, this should be enough to shut down an RPC client.
            this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            this.isShutdown = true;
        }
    }

    // TODO: implement this
    public boolean updateRecord()
    {
        return false;
    }

    public boolean insertRecord()
    {
        return false;
    }

    public boolean deleteRecord()
    {
        return false;
    }

    public void queryRecords()
    {

    }

    public void finishRecords()
    {

    }

    public void queryVisibility()
    {

    }

    public void finishVisibility()
    {

    }
}
