/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.daemon;

import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.daemon.exception.NoSuchServerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author hank
 */
public class ServerContainer
{
    private static Logger log = LogManager.getLogger(ServerContainer.class);

    private Map<String, Server> serverMap = null;

    public ServerContainer ()
    {
        this.serverMap = new HashMap<>();
    }

    public void addServer (String name, Server server)
    {
        Thread thread = new Thread(server);
        thread.start();
        this.serverMap.put(name, server);
    }

    public List<String> getServerNames()
    {
        return new ArrayList<>(this.serverMap.keySet());
    }

    /**
     * retry 3 times by default. sleep one second after each retry.
     * @param name
     * @return
     * @throws NoSuchServerException
     */
    public boolean checkServer(String name) throws NoSuchServerException
    {
        return this.checkServer(name, 3);
    }

    /**
     *
     * @param name
     * @param retry times to retry, sleep one second after each retry.
     * @return true if server is running.
     * @throws NoSuchServerException
     */
    public boolean checkServer(String name, int retry) throws NoSuchServerException
    {
        Server server = this.serverMap.get(name);
        if (server == null)
        {
            throw new NoSuchServerException();
        }
        boolean serverIsRunning = false;
        try
        {
            if (!server.isRunning())
            {
                for (int i = 0; i < retry; ++i)
                {
                    // try 3 times
                    TimeUnit.SECONDS.sleep(1);
                    if (server.isRunning())
                    {
                        serverIsRunning = true;
                        break;
                    }
                }
            }
            else
            {
                serverIsRunning = true;
            }
        } catch (InterruptedException e)
        {
            log.error(
                    "interrupted while checking server.", e);
        }
        return serverIsRunning;
    }

    public void startServer(String name) throws NoSuchServerException
    {
        this.shutdownServer(name);
        Thread serverThread = new Thread(this.serverMap.get(name));
        serverThread.start();
    }

    public void shutdownServer(String name) throws NoSuchServerException
    {
        Server server = this.serverMap.get(name);
        if (server == null)
        {
            throw new NoSuchServerException();
        }
        if (checkServer(name, 0) == true)
        {
            server.shutdown();
        }
    }
}
