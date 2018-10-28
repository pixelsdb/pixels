package cn.edu.ruc.iir.pixels.daemon.etcd;

import cn.edu.ruc.iir.pixels.common.lock.EtcdMutex;
import cn.edu.ruc.iir.pixels.common.lock.EtcdReadWriteLock;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import com.coreos.jetcd.Client;
import org.junit.Test;

/**
 * Created at: 18-10-28
 * Author: hank
 */
public class TestEtcdLock
{
    @Test
    public void delete ()
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        String basePath = "/read-write-lock";
        etcdUtil.deleteByPrefix(basePath);
    }

    @Test
    public void testReadLock () throws Exception
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        Client client = etcdUtil.getClient();
        String basePath = "/read-write-lock";

        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(client, basePath);
        EtcdMutex readLock = readWriteLock.readLock();
        readLock.acquire();
        System.err.println("get read lock");
        Thread.sleep(2000);
        System.err.println("release read lock");
        readLock.release();
    }

    @Test
    public void testWriteLock () throws Exception
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        Client client = etcdUtil.getClient();
        String basePath = "/read-write-lock";

        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(client, basePath);
        EtcdMutex writeLock = readWriteLock.writeLock();
        writeLock.acquire();
        System.err.println("get write lock");
        Thread.sleep(20000);
        System.err.println("release write lock");
        writeLock.release();
    }
}
