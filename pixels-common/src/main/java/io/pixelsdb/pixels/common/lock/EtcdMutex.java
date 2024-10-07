/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.common.lock;

import com.google.common.collect.Maps;
import io.etcd.jetcd.Client;
import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.utils.StringUtil;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author tao
 * @author hank
 * @create 2018-10-27 14:33
 **/
public class EtcdMutex implements InterProcessLock
{
    private final LockInternals internals;
    private final String basePath;

    private final ConcurrentMap<Thread, EtcdMutex.LockData> threadData;

    private static final String LOCK_NAME = "lock-";

    /**
     * @param client client
     * @param path the path to lock
     */
    public EtcdMutex(Client client, String path)
    {
        this(client, path, LOCK_NAME);
    }

    /**
     * @param client   client
     * @param path     the path to lock
     * @param lockName the lockName to lock
     */
    EtcdMutex(Client client, String path, String lockName)
    {
        this.threadData = Maps.newConcurrentMap();
        this.basePath = StringUtil.validatePath(path);
        internals = new LockInternals(client, path, lockName);
    }

    public EtcdMutex verbose(boolean verbose)
    {
        this.internals.verbose(verbose);
        return this;
    }

    public EtcdMutex verbose()
    {
        return verbose(true);
    }

    /**
     * Acquire the mutex - blocking until it's available. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws EtcdException errors, connection interruptions
     */
    public void acquire() throws EtcdException
    {
        try
        {
            if (!this.internalLock(Long.MAX_VALUE, TimeUnit.SECONDS))
            {
                throw new EtcdException("Lost connection while trying to acquire lock: " + this.basePath);
            }
        }
        catch (EtcdException e)
        {
            throw new EtcdException("failed to acquire lock", e);
        }
    }

    private boolean internalLock(long time, TimeUnit unit) throws EtcdException
    {
        Thread currentThread = Thread.currentThread();
        EtcdMutex.LockData lockData = this.threadData.get(currentThread);
        if (lockData != null)
        {
            lockData.lockCount.incrementAndGet();
            return true;
        }
        else
        {
            String lockPath = internals.attemptLock(time, unit);
            if (lockPath != null)
            {
                EtcdMutex.LockData newLockData = new EtcdMutex.LockData(currentThread, lockPath);
                this.threadData.put(currentThread, newLockData);
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    /**
     * Acquire the mutex - blocks until it is available or the given time expires. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws EtcdException errors, connection interruptions
     */
    public boolean acquire(long time, TimeUnit unit) throws EtcdException
    {
        return this.internalLock(time, unit);
    }

    /**
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    public boolean isAcquiredInThisProcess()
    {
        return !this.threadData.isEmpty();
    }

    /**
     * Perform one release of the mutex if the calling thread is the same thread that acquired it. If the
     * thread had made multiple calls to acquire, the mutex will still be held when this method returns.
     *
     * @throws EtcdException errors, interruptions, current thread does not own the lock
     */
    public void release() throws EtcdException
    {
        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        if (lockData == null)
        {
            // don't throw exception if the lock is not owned by this thread
            return;
        }

        int newLockCount = lockData.lockCount.decrementAndGet();
        if (newLockCount > 0)
        {
            return;
        }
        if (newLockCount < 0)
        {
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + basePath);
        }
        try
        {
            internals.releaseLock(lockData.lockPath);
        }
        finally
        {
            threadData.remove(currentThread);
        }
    }

    private static class LockData
    {
        final Thread owningThread;
        final String lockPath;
        final AtomicInteger lockCount;

        private LockData(Thread owningThread, String lockPath)
        {
            this.lockCount = new AtomicInteger(1);
            this.owningThread = owningThread;
            this.lockPath = lockPath;
        }
    }
}
