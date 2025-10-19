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
package io.pixelsdb.pixels.index.rocksdb;

import org.rocksdb.ReadOptions;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * ThreadFactory that manages a ThreadLocal ReadOptions for each thread.
 * Each thread holds its own ReadOptions instance, which will be
 * automatically released when the thread exits.
 *
 * This avoids frequent JNI allocation/deallocation and ensures
 * proper native resource cleanup on thread or system shutdown.
 */
public class ReadOptionsThreadFactory implements ThreadFactory
{
    private final ThreadFactory delegate = Executors.defaultThreadFactory();

    /**
     * Keep weak references to all threads created by this factory
     * for cleanup on JVM shutdown.
     */
    private final Set<Thread> createdThreads =
            Collections.newSetFromMap(new WeakHashMap<>());

    /**
     * Thread-local ReadOptions for each thread.
     */
    private final ThreadLocal<ReadOptions> threadLocalReadOptions =
            ThreadLocal.withInitial(ReadOptions::new);

    public ReadOptionsThreadFactory()
    {
        // Register JVM shutdown hook to close any remaining ReadOptions
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownAllThreads));
    }

    @Override
    public Thread newThread(Runnable r)
    {
        Thread thread = delegate.newThread(() -> {
            try {
                r.run();
            } finally {
                // When the thread exits, close its ReadOptions
                ReadOptions opt = threadLocalReadOptions.get();
                if (opt != null) {
                    try {
                        opt.close();
                    } catch (Exception ignored) {
                        // ignore cleanup errors
                    } finally {
                        threadLocalReadOptions.remove();
                    }
                }
            }
        });

        createdThreads.add(thread);
        return thread;
    }

    /**
     * Get the current thread's ReadOptions.
     * The ReadOptions is lazily created on first access.
     */
    public ReadOptions getReadOptions()
    {
        return threadLocalReadOptions.get();
    }

    /**
     * Forcefully close all ReadOptions from all created threads.
     * Should be called during system shutdown to ensure no leaks.
     */
    public void shutdownAllThreads()
    {
        for (Thread t : createdThreads)
        {
            if (!t.isAlive())
            {
                ReadOptions opt = threadLocalReadOptions.get();
                if (opt != null)
                {
                    try {
                        opt.close();
                    } catch (Exception ignored) {
                    } finally {
                        threadLocalReadOptions.remove();
                    }
                }
            }
        }
    }
}