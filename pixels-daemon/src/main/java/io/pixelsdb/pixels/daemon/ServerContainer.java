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
import java.util.Arrays;
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

    private static final class ServerHandle
    {
        private final Server server;
        private final List<StartupCheck> startupChecks;
        private Thread thread;
        private boolean shutdownInvoked;

        private ServerHandle(Server server, List<StartupCheck> startupChecks)
        {
            this.server = server;
            this.startupChecks = startupChecks;
        }
    }
    private final Map<String, ServerHandle> serverHandles;
    /**
     * Once shutdown begins, no server thread can be (re)started in this container,
     * otherwise the daemon main loop would resurrect the servers that are being shutdown.
     */
    private boolean shuttingDown = false;

    public ServerContainer ()
    {
        this.serverHandles = new HashMap<>();
    }

    public synchronized void addServer (
            String name, Server server, StartupCheck... startupChecks)
    {
        if (this.shuttingDown)
        {
            throw new IllegalStateException("server container is shutting down");
        }
        if (this.serverHandles.containsKey(name))
        {
            throw new IllegalArgumentException("server is already registered: " + name);
        }
        this.serverHandles.put(name, new ServerHandle(server, Arrays.asList(startupChecks)));
        startServerThread(name);
    }

    public synchronized List<String> getServerNames()
    {
        return new ArrayList<>(this.serverHandles.keySet());
    }

    /**
     * Ensure that a server has one thread responsible for its lifecycle.
     * A server may still be waiting for startup checks while its
     * {@link Server#isRunning()} method returns false.
     */
    public synchronized void startServer(String name) throws NoSuchServerException
    {
        ServerHandle handle = this.serverHandles.get(name);
        if (handle == null)
        {
            throw new NoSuchServerException();
        }
        if (this.shuttingDown)
        {
            log.debug("Server container is shutting down, skip starting {}", name);
            return;
        }
        Thread serverThread = handle.thread;
        if ((serverThread != null && serverThread.isAlive())
                || handle.server.isRunning())
        {
            log.debug("Server {} is already starting or running, skip duplicate start", name);
            return;
        }
        startServerThread(name);
    }

    /**
     * Check whether the thread responsible for a server's lifecycle is alive.
     */
    public synchronized boolean checkServer(String name)
            throws NoSuchServerException
    {
        ServerHandle handle = this.serverHandles.get(name);
        if (handle == null)
        {
            throw new NoSuchServerException();
        }
        Thread serverThread = handle.thread;
        return serverThread != null && serverThread.isAlive();
    }

    /**
     * Atomically prevent new server starts and initiate shutdown of every registered server.
     */
    public void shutdownAll()
    {
        Map<String, ServerHandle> handlesToShutdown;
        synchronized (this)
        {
            if (this.shuttingDown)
            {
                return;
            }
            this.shuttingDown = true;
            handlesToShutdown = new HashMap<>(this.serverHandles);
        }

        for (Map.Entry<String, ServerHandle> entry : handlesToShutdown.entrySet())
        {
            shutdownServer(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Wait until all lifecycle threads have exited and all servers report stopped.
     *
     * @return true if every server stopped before the timeout, false otherwise
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        if (timeout < 0)
        {
            throw new IllegalArgumentException("timeout is negative");
        }
        long timeoutNanos = unit.toNanos(timeout);
        long startNanos = System.nanoTime();

        while (true)
        {
            Map<String, ServerHandle> handles;
            boolean shutdownStarted;
            synchronized (this)
            {
                handles = new HashMap<>(this.serverHandles);
                shutdownStarted = this.shuttingDown;
            }

            List<String> unterminatedServers = new ArrayList<>();
            for (Map.Entry<String, ServerHandle> entry : handles.entrySet())
            {
                String name = entry.getKey();
                ServerHandle handle = entry.getValue();
                boolean running = isServerRunning(name, handle);
                if (shutdownStarted && running)
                {
                    invokeShutdown(name, handle);
                }
                Thread serverThread = getServerThread(handle);
                if ((serverThread != null && serverThread.isAlive()) || running)
                {
                    unterminatedServers.add(name);
                }
            }

            if (unterminatedServers.isEmpty())
            {
                return true;
            }

            long remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
            if (remainingNanos <= 0)
            {
                log.warn("Timed out waiting for servers to stop: {}", unterminatedServers);
                return false;
            }

            synchronized (this)
            {
                TimeUnit.NANOSECONDS.timedWait(
                        this, Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(100)));
            }
        }
    }

    private void shutdownServer(String name, ServerHandle handle)
    {
        if (isServerRunning(name, handle))
        {
            invokeShutdown(name, handle);
        }
        else
        {
            interruptServerThread(handle);
        }
    }

    private void invokeShutdown(String name, ServerHandle handle)
    {
        synchronized (this)
        {
            if (handle.shutdownInvoked)
            {
                return;
            }
            handle.shutdownInvoked = true;
        }
        try
        {
            handle.server.shutdown();
        }
        catch (Throwable e)
        {
            log.error("Failed to shutdown server {}", name, e);
        }
        finally
        {
            interruptServerThread(handle);
        }
    }

    private boolean isServerRunning(String name, ServerHandle handle)
    {
        try
        {
            return handle.server.isRunning();
        }
        catch (Throwable e)
        {
            log.error("Failed to check whether server {} is running", name, e);
            return true;
        }
    }

    private synchronized Thread getServerThread(ServerHandle handle)
    {
        return handle.thread;
    }

    private void interruptServerThread(ServerHandle handle)
    {
        Thread serverThread = getServerThread(handle);
        if (serverThread != null && serverThread.isAlive()
                && serverThread != Thread.currentThread())
        {
            serverThread.interrupt();
        }
    }

    private void startServerThread(String name)
    {
        ServerHandle handle = this.serverHandles.get(name);
        if (handle == null)
        {
            throw new IllegalStateException("server is not registered: " + name);
        }
        Thread existingThread = handle.thread;
        if (existingThread != null && existingThread.isAlive())
        {
            return;
        }

        handle.thread = new Thread(() ->
        {
            try
            {
                long startupDeadline = System.nanoTime() + 60_000_000_000L;
                for (StartupCheck startupCheck : handle.startupChecks)
                {
                    long remainingNanos = startupDeadline - System.nanoTime();
                    if (remainingNanos <= 0)
                    {
                        throw new IllegalStateException(
                                "Timed out waiting for startup checks of " + name
                                        + " after 60 seconds");
                    }
                    log.debug("Server {} is waiting for {}", name, startupCheck.getDescription());
                    startupCheck.awaitReady(startupDeadline);
                    if (System.nanoTime() >= startupDeadline)
                    {
                        throw new IllegalStateException(
                                "Timed out waiting for startup checks of " + name
                                        + " after 60 seconds");
                    }
                }
                synchronized (ServerContainer.this)
                {
                    if (ServerContainer.this.shuttingDown
                            || Thread.currentThread().isInterrupted())
                    {
                        return;
                    }
                }
                handle.server.run();
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                log.info("Server {} startup was interrupted", name);
            }
            catch (Throwable e)
            {
                log.error("Server {} failed during startup or execution", name, e);
            }
            finally
            {
                boolean shutdownStarted;
                synchronized (ServerContainer.this)
                {
                    shutdownStarted = ServerContainer.this.shuttingDown;
                }
                if (shutdownStarted)
                {
                    invokeShutdown(name, handle);
                }
                synchronized (ServerContainer.this)
                {
                    if (handle.thread == Thread.currentThread())
                    {
                        handle.thread = null;
                    }
                    ServerContainer.this.notifyAll();
                }
            }
        }, name);
        handle.thread.start();
    }
}
