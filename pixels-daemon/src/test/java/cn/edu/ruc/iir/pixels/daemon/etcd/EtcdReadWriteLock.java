package cn.edu.ruc.iir.pixels.daemon.etcd;

import com.coreos.jetcd.Client;
import org.apache.curator.framework.recipes.locks.PredicateResults;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.etcd
 * @ClassName: EtcdReadWriteLock
 * @Description:
 * @author: tao
 * @date: Create in 2018-10-27 14:29
 **/
public class EtcdReadWriteLock {
    private final EtcdMutex readMutex;
    private final EtcdMutex writeMutex;
    private static final String READ_LOCK_NAME = "__READ__";
    private static final String WRITE_LOCK_NAME = "__WRIT__";

    public EtcdReadWriteLock(Client client, String basePath) {
        this(client, basePath, (byte[]) null);
    }

    public EtcdReadWriteLock(Client client, String basePath, byte[] lockData) {
        lockData = lockData == null ? null : Arrays.copyOf(lockData, lockData.length);
        this.writeMutex = new EtcdReadWriteLock.InternalInterProcessMutex(client, basePath, "__WRIT__", lockData, 1);
        this.readMutex = new EtcdReadWriteLock.InternalInterProcessMutex(client, basePath, "__READ__", lockData, 2147483647);
    }

    public EtcdMutex readLock() {
        return this.readMutex;
    }

    public EtcdMutex writeLock() {
        return this.writeMutex;
    }



    private PredicateResults readLockPredicate(List<String> children, String sequenceNodeName) throws Exception {
        if (this.writeMutex.isOwnedByCurrentThread()) {
            return new PredicateResults((String) null, true);
        } else {
            int index = 0;
            int firstWriteIndex = 2147483647;
            int ourIndex = -1;

            String node;
            for (Iterator var6 = children.iterator(); var6.hasNext(); ++index) {
                node = (String) var6.next();
                if (node.contains("__WRIT__")) {
                    firstWriteIndex = Math.min(index, firstWriteIndex);
                } else if (node.startsWith(sequenceNodeName)) {
                    ourIndex = index;
                    break;
                }
            }

//            StandardLockInternalsDriver.validateOurIndex(sequenceNodeName, ourIndex);
            boolean getsTheLock = ourIndex < firstWriteIndex;
            node = getsTheLock ? null : (String) children.get(firstWriteIndex);
            return new PredicateResults(node, getsTheLock);
        }
    }

    private static class InternalInterProcessMutex extends EtcdMutex {
        private final String lockName;
        private final byte[] lockData;

        InternalInterProcessMutex(Client client, String path, String lockName, byte[] lockData, int maxLeases) {
            super(client, path, lockName, maxLeases);
            this.lockName = lockName;
            this.lockData = lockData;
        }

        protected byte[] getLockNodeBytes() {
            return this.lockData;
        }
    }

}
