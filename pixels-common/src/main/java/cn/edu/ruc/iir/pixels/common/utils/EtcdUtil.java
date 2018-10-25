package cn.edu.ruc.iir.pixels.common.utils;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.DeleteResponse;
import com.coreos.jetcd.kv.PutResponse;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.options.WatchOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Created at: 18-10-14
 * Author: hank
 */
public class EtcdUtil {
    private static EtcdUtil instance = new EtcdUtil();
    private static Logger logger = LoggerFactory.getLogger(EtcdUtil.class);
    private Client client = null;
    private boolean lockHeld;

    private EtcdUtil() {
        String[] hosts = ConfigFactory.Instance().getProperty("etcd.hosts").split(",");
        Random random = new Random(System.nanoTime());
        String host = hosts[random.nextInt(hosts.length)];
        System.out.println(host);
        String port = ConfigFactory.Instance().getProperty("etcd.port");

        if (this.client == null) {
            this.client = Client.builder().endpoints("http://" + host + ":" + port).build();
        }
    }

    public static EtcdUtil Instance() {
        return instance;
    }

    /**
     * get the singleton client instance.
     *
     * @return
     */
    public Client getClient() {
        return client;
    }

    /**
     * get key-value by key from etcd.
     * you should ensure that there is only one value for this key.
     *
     * @param key etcdKey
     * @return
     */
    public KeyValue getKeyValue(String key) {
        KeyValue keyValue = null;
        try {
            List<KeyValue> keyValues = this.client.getKVClient().get(ByteSequence.fromString(key)).get().getKvs();
            if (keyValues.size() > 0) {
                keyValue = keyValues.get(0);
            }
        } catch (Exception e) {
            logger.error("error when get key-value by key.", e);
        }
        return keyValue;
    }

    /**
     * get all key-values with this prefix.
     *
     * @param prefix the prefix
     * @return
     */
    public List<KeyValue> getKeyValuesByPrefix(String prefix) {
        List<KeyValue> keyValues = new ArrayList<>();
        GetOption getOption = GetOption.newBuilder().withPrefix(ByteSequence.fromString(prefix)).build();
        try {
            keyValues = this.client.getKVClient().get(ByteSequence.fromString(prefix), getOption).get().getKvs();
        } catch (Exception e) {
            logger.error("error when get key-values by prefix.", e);
        }
        return keyValues;
    }

    /**
     * put key-value into etcd.
     *
     * @param key
     * @param value
     */
    public void putKeyValue(String key, String value) {
        CompletableFuture<PutResponse> future = client.getKVClient().put(ByteSequence.fromString(key), ByteSequence.fromString(value));
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("error when put key-value into etcd.", e);
        }
    }

    /**
     * put key-value into etcd with an expire time (by etcd lease).
     *
     * @param key
     * @param value
     * @param expireTime expire time in seconds.
     * @return lease id, 0L if error occurs.
     */
    public long putKeyValueWithExpireTime(String key, String value, long expireTime) {
        CompletableFuture<LeaseGrantResponse> leaseGrantResponse = this.client.getLeaseClient().grant(expireTime);
        PutOption putOption;
        try {
            putOption = PutOption.newBuilder().withLeaseId(leaseGrantResponse.get().getID()).build();
            this.client.getKVClient().put(ByteSequence.fromString(key), ByteSequence.fromString(value), putOption);
            return leaseGrantResponse.get().getID();
        } catch (Exception e) {
            logger.error("error when put key-value with expire time into etcd.", e);
        }
        return 0L;
    }

    /**
     * put key-value with a lease id.
     *
     * @param key
     * @param value
     * @param leaseId lease id
     * @return revision id, 0L if error occurs.
     */
    public long putKeyValueWithLeaseId(String key, String value, long leaseId) throws Exception {
        PutOption putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
        CompletableFuture<PutResponse> putResponse = this.client.getKVClient().put(ByteSequence.fromString(key), ByteSequence.fromString(value), putOption);
        try {
            return putResponse.get().getHeader().getRevision();
        } catch (Exception e) {
            logger.error("error when put key-value with lease id into etcd.", e);
        }
        return 0L;
    }

    /**
     * keep a lease alive.
     *
     * @param leaseId
     */
    public void keepLeaseAlive(long leaseId) {
        this.client.getLeaseClient().keepAlive(leaseId);
    }

    /**
     * delete key-value by key.
     *
     * @param key
     */
    public void delete(String key) {
        this.client.getKVClient().delete(ByteSequence.fromString(key));
    }

    /**
     * delete all key-values with this prefix.
     *
     * @param prefix
     */
    public void deleteByPrefix(String prefix) {
        DeleteOption deleteOption = DeleteOption.newBuilder().withPrefix(ByteSequence.fromString(prefix)).build();
        this.client.getKVClient().delete(ByteSequence.fromString(prefix), deleteOption);
    }

    /**
     * get the custom watcher of the key.
     *
     * @param key
     * @return
     */
    public Watch.Watcher getCustomWatcherForKey(String key) {
        return this.client.getWatchClient().watch(ByteSequence.fromString(key));
    }

    /**
     * get a watcher who watches the set of keys with the same prefix.
     *
     * @param prefix
     * @return
     */
    public Watch.Watcher getCustomWatcherForPrefix(String prefix) {
        WatchOption watchOption = WatchOption.newBuilder().withPrefix(ByteSequence.fromString(prefix)).build();
        return this.client.getWatchClient().watch(ByteSequence.fromString(prefix), watchOption);
    }

    private String name;

    /**
     * Attempt to acquire a lock.
     *
     * @return boolean indicating success
     */
    synchronized public boolean acquire(String name, String lockContent) {
        this.name = name;
        CompletableFuture<PutResponse> future = client.getKVClient().put(ByteSequence.fromString(name), ByteSequence.fromString(lockContent));
        PutResponse response = null;
        try {
            response = future.get();
            lockHeld = true;
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Error encountered when attempting to acquire lock", e);
            lockHeld = false;
        }
        return lockHeld;
    }

    synchronized public boolean renew(String name, String lockContent) throws Exception {
        // validate that we have a lock
        if (!lockHeld) {
            throw new Exception("Lock cannot be released unless first acquired");
        }

        CompletableFuture<PutResponse> future = client.getKVClient().put(ByteSequence.fromString(name), ByteSequence.fromString(lockContent));
        PutResponse response = null;
        try {
            response = future.get();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Error encountered when attempting to acquire lock", e);
        }
        return true;
    }

    /**
     * Release the currently held lock.
     *
     * @return boolean indicating success
     * @throws Exception If no lock is held
     */
    synchronized public boolean release() throws Exception {
        // validate that we have a lock
        if (!lockHeld) {
            throw new Exception("Lock cannot be released unless first acquired");
        }
        // attempt to release the lock
        DeleteResponse response;
        try {
            response = client.getKVClient().delete(ByteSequence.fromString(name)).get();
            lockHeld = false;
            return true;
        } catch (Exception e) {
            logger.error("Lock could not be released", e);
        }
        return false;
    }

    public void close() {
        // attempt to close the connection
        if (lockHeld) {
            try {
                release();
            } catch (Exception e) {
                logger.error("Lock could not be released", e);
            }
        }
        // close the Etcd connection
        client.close();
    }
}