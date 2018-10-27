package cn.edu.ruc.iir.pixels.daemon.etcd;

import com.coreos.jetcd.Client;
import com.google.common.collect.Maps;
import org.apache.curator.framework.recipes.locks.LockInternalsDriver;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.etcd
 * @ClassName: EtcdMutex
 * @Description:
 * @author: tao
 * @date: Create in 2018-10-27 14:33
 **/
public class EtcdMutex implements InterProcessLock {
    private final String basePath;
    private final ConcurrentMap<Thread, EtcdMutex.LockData> threadData;
    private static final String LOCK_NAME = "lock-";
    private final String path;

    public EtcdMutex(Client client, String path) {
        this(client, path, new StandardLockInternalsDriver());
    }

    public EtcdMutex(Client client, String path, LockInternalsDriver driver) {
        this(client, path, "lock-", 1);
    }

    EtcdMutex(Client client, String path, String lockName, int maxLeases) {
        this.threadData = Maps.newConcurrentMap();
        this.basePath = PathUtils.validatePath(path);
        this.path = ZKPaths.makePath(path, lockName);
    }

    boolean isOwnedByCurrentThread() {
        EtcdMutex.LockData lockData = (EtcdMutex.LockData) this.threadData.get(Thread.currentThread());
        return lockData != null && lockData.lockCount.get() > 0;
    }

    public void acquire() throws Exception {
        if (!this.internalLock(-1L, (TimeUnit) null)) {
            throw new IOException("Lost connection while trying to acquire lock: " + this.basePath);
        }

    }

    protected byte[] getLockNodeBytes() {
        return null;
    }

    private boolean internalLock(long time, TimeUnit unit) throws Exception {
        Thread currentThread = Thread.currentThread();
        EtcdMutex.LockData lockData = (EtcdMutex.LockData) this.threadData.get(currentThread);
        if (lockData != null) {
            lockData.lockCount.incrementAndGet();
            return true;
        } else {
            String lockPath = null;
//            String lockPath = this.attemptLock(time, unit, this.getLockNodeBytes());
            if (lockPath != null) {
//                EtcdMutex.LockData newLockData = new EtcdMutex.LockData(currentThread, lockPath, null);
//                this.threadData.put(currentThread, newLockData);
                return true;
            } else {
                return false;
            }
        }
    }

    public boolean acquire(long time, TimeUnit unit) throws Exception {
        return this.internalLock(time, unit);
    }

    public boolean isAcquiredInThisProcess() {
        return this.threadData.size() > 0;
    }

    public void release() throws Exception {

    }

    private static class LockData {
        final Thread owningThread;
        final String lockPath;
        final AtomicInteger lockCount;

        private LockData(Thread owningThread, String lockPath) {
            this.lockCount = new AtomicInteger(1);
            this.owningThread = owningThread;
            this.lockPath = lockPath;
        }
    }


}
