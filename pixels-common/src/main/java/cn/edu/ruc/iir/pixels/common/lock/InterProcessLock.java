package cn.edu.ruc.iir.pixels.common.lock;

import java.util.concurrent.TimeUnit;

public interface InterProcessLock {

    /**
     * Acquire the mutex - blocking until it's available. Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws Exception errors, connection interruptions
     */
    void acquire() throws Exception;

    /**
     * Acquire the mutex - blocks until it's available or the given time expires. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param var1 time to wait
     * @param var3 time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception errors, connection interruptions
     */
    boolean acquire(long var1, TimeUnit var3) throws Exception;

    /**
     * Perform one release of the mutex.
     *
     * @throws Exception errors, interruptions, current thread does not own the lock
     */
    void release() throws Exception;

    /**
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    boolean isAcquiredInThisProcess();
}
