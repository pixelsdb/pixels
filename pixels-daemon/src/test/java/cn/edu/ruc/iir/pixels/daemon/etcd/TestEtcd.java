package cn.edu.ruc.iir.pixels.daemon.etcd;

import cn.edu.ruc.iir.pixels.common.lock.EtcdMutex;
import cn.edu.ruc.iir.pixels.common.lock.EtcdReadWriteLock;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
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
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        String key = "etcd";
        long start = System.currentTimeMillis();
        //EtcdUtil.putEtcdKey(key, "hello world");
        KeyValue keyValue = etcdUtil.getKeyValue(key);
        long end = System.currentTimeMillis();
        System.out.println((end - start));
        if (keyValue != null)
            System.out.println("keyValue is：" + keyValue.getValue().toStringUtf8());
        else
            System.out.println("keyValue is：" + keyValue);
    }

    @Test
    public void testLock() throws Exception {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        Client client = etcdUtil.getClient();
        String basePath = "/read-write-lock";

        etcdUtil.deleteByPrefix(basePath);

        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(client, basePath);

        //读锁
        final EtcdMutex readLock = readWriteLock.readLock();
        //写锁
        final EtcdMutex writeLock = readWriteLock.writeLock();

        final EtcdMutex readLock1 = readWriteLock.readLock();

        final EtcdMutex readLock2 = readWriteLock.readLock();

        final EtcdMutex writeLock1 = readWriteLock.writeLock();

        try {
            readLock.acquire();
            System.out.println(Thread.currentThread() + "获取到读锁-0");

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        //在读锁没释放之前不能读取写锁。
                        writeLock.acquire();
                        System.out.println(Thread.currentThread() + "获取到写锁-0");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            writeLock.release();
                            System.out.println(Thread.currentThread() + "writeLock 释放写锁-0");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        readLock1.acquire();
                        System.out.println(Thread.currentThread() + "readLock1 获取到读锁-1");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            Thread.sleep(4000);
                            readLock1.release();
                            System.out.println(Thread.currentThread() + "readLock1 释放读锁-1");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        readLock2.acquire();
                        System.out.println(Thread.currentThread() + "readLock2 获取到读锁-2");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            Thread.sleep(2000);
                            readLock2.release();
                            System.out.println(Thread.currentThread() + "readLock2 释放读锁-2");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        //在读锁没释放之前不能读取写锁。
                        writeLock1.acquire();
                        System.out.println(Thread.currentThread() + "writeLock1 获取到写锁-1");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            writeLock1.release();
                            System.out.println(Thread.currentThread() + "writeLock1 释放写锁-1");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();

            //停顿3000毫秒不释放锁，这时其它线程可以获取读锁，却不能获取写锁。
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //停顿3000毫秒不释放锁，这时其它线程可以获取读锁，却不能获取写锁。
            Thread.sleep(8000);
            System.out.println(Thread.currentThread() + "readLock 释放读锁-0");
            readLock.release();
        }

        Thread.sleep(30000);
        System.out.println("End");
    }

}
