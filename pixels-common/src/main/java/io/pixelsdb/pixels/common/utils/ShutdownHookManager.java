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
package io.pixelsdb.pixels.common.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2025-11-07
 */
public class ShutdownHookManager
{
    private static final Logger logger = LogManager.getLogger(ShutdownHookManager.class);

    private static final ShutdownHookManager instance = new ShutdownHookManager();

    public static ShutdownHookManager Instance()
    {
        return instance;
    }

    static
    {
        ExecutorService serialHookRunner = Executors.newSingleThreadExecutor();
        ExecutorService concurrentHookRunner = Executors.newCachedThreadPool();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (ShutdownHook hook : instance.shutdownHooks)
            {
                if (hook.serial)
                {
                    serialHookRunner.submit(hook);
                }
                else
                {
                    concurrentHookRunner.submit(hook);
                }
            }
            serialHookRunner.shutdown();
            concurrentHookRunner.shutdown();
            try
            {
                // All shutdown hooks should terminate within 60 seconds.
                if (!concurrentHookRunner.awaitTermination(59, TimeUnit.SECONDS))
                {
                    concurrentHookRunner.shutdownNow();
                }
                if (!serialHookRunner.awaitTermination(1, TimeUnit.SECONDS))
                {
                    serialHookRunner.shutdownNow();
                }
            }
            catch (InterruptedException e)
            {
                logger.error("interrupted while waiting for shutdown hooks to terminate", e);
            }
        }));
    }

    private final Queue<ShutdownHook> shutdownHooks = new ConcurrentLinkedQueue<>();

    public void registerShutdownHook(Class<?> clazz, boolean serial, Runnable runnable)
    {?
        this.shutdownHooks.offer(new ShutdownHook(clazz, serial, runnable));
    }

    public static class ShutdownHook implements Runnable
    {
        private final Class<?> clazz;
        private final boolean serial;
        private final Runnable hook;

        public ShutdownHook(Class<?> clazz, boolean serial, Runnable hook)
        {
            this.clazz = clazz;
            this.serial = serial;
            this.hook = hook;
        }

        @Override
        public void run()
        {
            this.hook.run();
        }
    }
}
