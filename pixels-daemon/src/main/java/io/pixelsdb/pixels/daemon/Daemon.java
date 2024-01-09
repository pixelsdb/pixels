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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 */
public class Daemon
{
    private FileChannel myChannel = null;
    private FileLock myLock = null;
    private volatile boolean running = false;
    private volatile boolean cleaned = false;
    private final ShutdownHandler shutdownHandler = null;
    private static final Logger log = LogManager.getLogger(Daemon.class);

    public void setup (String lockFilePath)
    {
        File myLockFile = new File(lockFilePath);
        if (!myLockFile.exists())
        {
            try
            {
                if (!myLockFile.createNewFile())
                {
                    log.info("reuse existing lock file " + lockFilePath);
                }
            } catch (IOException e)
            {
                log.error("failed to create lock file " + lockFilePath, e);
            }
        }
        try
        {
            this.myChannel = new FileOutputStream(myLockFile).getChannel();
            this.myLock = this.myChannel.tryLock();
            if (myLock == null)
            {
                // this process has been started
                this.clean();
                log.info("another daemon process is holding lock on " + lockFilePath + ", exiting...");
                this.running = false;
                System.exit(0);
            }
            else
            {
                this.running = true;
                log.info("starting daemon thread...");
            }

            /**
             * Issue #181:
             * We should not bind the SIGTERM handler.
             * If it is bind, the shutdown hooks will not be called.
             * Daemon.shutdown() will be called in the shutdown hook in DaemonMain,
             * instead of in the signal handler.
             */
            // bind handler for SIGTERM(15) signal.
            //this.shutdownHandler = new ShutdownHandler(this);
            //Signal.handle(new Signal("TERM"), shutdownHandler);
        } catch (IOException e)
        {
            log.error("I/O exception when creating lock file channels.", e);
            this.clean();
        }
    }

    public void clean ()
    {
        try
        {
            if (myLock != null)
            {
                myLock.release();
            }
        } catch (IOException e1)
        {
            log.error("error when releasing daemon lock");
        }

        try
        {
            if (this.myChannel != null)
            {
                this.myChannel.truncate(0);
                this.myChannel.close();
            }
        } catch (IOException e)
        {
            log.error("error when closing my own channel.", e);
        }
        this.cleaned = true;
    }

    public void shutdown()
    {
        this.running = false;
        while (!this.cleaned)
        {
            log.info("the daemon thread is stopped, cleaning the file channels...");
            this.clean();
        }
    }

    public boolean isRunning()
    {
        return this.running;
    }

    public static class ShutdownHandler implements SignalHandler
    {
        private final Daemon daemon;
        private final Runnable executor;

        public ShutdownHandler(Daemon daemon, Runnable executor)
        {
            this.daemon = requireNonNull(daemon, "daemon is null");
            this.executor = requireNonNull(executor, "shutdown executor is null");
        }

        @Override
        public void handle(Signal signal)
        {
            if (signal.getNumber() == 15)
            {
                this.daemon.shutdown();
                this.executor.run();
            }
        }
    }
}
