package io.pixelsdb.pixels.common.lock;

import com.coreos.jetcd.Client;
import com.google.common.collect.Maps;
import org.apache.curator.utils.PathUtils;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: tao
 * @date: Create in 2018-10-27 14:33
 **/
public class EtcdMutex implements InterProcessLock
{
    private final LockInternals internals;
    private final String basePath;

    private final ConcurrentMap<Thread, EtcdMutex.LockData> threadData;

    private static final String LOCK_NAME = "lock-";

    /**
     * @param client client
     * @param path   the path to lock
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
        this.basePath = PathUtils.validatePath(path);
        internals = new LockInternals(client, path, lockName);
    }

    /**
     * Acquire the mutex - blocking until it's available. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws Exception errors, connection interruptions
     */
    public void acquire() throws Exception
    {
        if (!this.internalLock(-1L, (TimeUnit) null))
        {
            throw new IOException("Lost connection while trying to acquire lock: " + this.basePath);
        }

    }

    private boolean internalLock(long time, TimeUnit unit) throws Exception
    {
        Thread currentThread = Thread.currentThread();
        EtcdMutex.LockData lockData = (EtcdMutex.LockData) this.threadData.get(currentThread);
        if (lockData != null)
        {
            lockData.lockCount.incrementAndGet();
            return true;
        }
        else
        {
            String lockPath = internals.attemptLock(time, unit);
            System.out.println("[attemptLock]: end, " + lockPath);
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
     * Acquire the mutex - blocks until it's available or the given time expires. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception errors, connection interruptions
     */
    public boolean acquire(long time, TimeUnit unit) throws Exception
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
        return this.threadData.size() > 0;
    }

    /**
     * Perform one release of the mutex if the calling thread is the same thread that acquired it. If the
     * thread had made multiple calls to acquire, the mutex will still be held when this method returns.
     *
     * @throws Exception errors, interruptions, current thread does not own the lock
     */
    public void release() throws Exception
    {
        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        if (lockData == null)
        {
            throw new IllegalMonitorStateException("You do not own the lock: " + basePath);
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
