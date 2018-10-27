package cn.edu.ruc.iir.pixels.daemon.etcd;

import cn.edu.ruc.iir.pixels.daemon.etcd.util.EtcdUtil;
import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.etcd
 * @ClassName: TestEtcd
 * @Description:
 * @author: tao
 * @date: Create in 2018-09-28 11:40
 **/
public class TestEtcd {

    @Test
    public void testEtcd() throws ExecutionException, InterruptedException {
        // create client
        Client client = Client.builder().endpoints("http://presto04:2379").build();
        KV kvClient = client.getKVClient();

        ByteSequence key = ByteSequence.fromString("test_key");
        ByteSequence value = ByteSequence.fromString("test_value");

        long start = System.currentTimeMillis();
        // put the key-value
        kvClient.put(key, value).get();


        // get the CompletableFuture
        CompletableFuture<GetResponse> getFuture = kvClient.get(key);
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        // get the value from CompletableFuture
        GetResponse response = getFuture.get();
    }

    @Test
    public void testGetEtcdKey() {
        String address = "10.77.40.238";
        EtcdUtil.ClientInit(address);
        String key = "etcd";
        long start = System.currentTimeMillis();
        //EtcdUtil.putEtcdKey(key, "hello world");
        KeyValue keyValue = EtcdUtil.getEtcdKey(key);
        long end = System.currentTimeMillis();
        System.out.println((end - start));
        if (keyValue != null)
            System.out.println("keyValue is：" + keyValue.getValue().toStringUtf8());
        else
            System.out.println("keyValue is：" + keyValue);
    }

    @Test
    public void testLock() {
        cn.edu.ruc.iir.pixels.common.utils.EtcdUtil etcdUtil = cn.edu.ruc.iir.pixels.common.utils.EtcdUtil.Instance().Instance();
        Client client = etcdUtil.getClient();
        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(client, "/read-write-lock");

        System.out.println(readWriteLock.toString());

        //读锁
        final EtcdMutex readLock = readWriteLock.readLock();
        //写锁
        final EtcdMutex writeLock = readWriteLock.writeLock();

//        readLock.acquire();
        System.out.println(Thread.currentThread() + "获取到读锁");
    }

    @Test
    public void testKV()
    {
        cn.edu.ruc.iir.pixels.common.utils.EtcdUtil etcdUtil = cn.edu.ruc.iir.pixels.common.utils.EtcdUtil.Instance();
        KV client = etcdUtil.getClient().getKVClient();
//        client.get()
    }

}
