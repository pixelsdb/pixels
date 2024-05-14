/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.common.utils;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @create 2018-10-14
 */
public class EtcdUtil
{
    private static final Logger logger = LogManager.getLogger(EtcdUtil.class);
    private static final EtcdUtil instance = new EtcdUtil();
    private Client client = null;
    private boolean lockHeld;

    private EtcdUtil()
    {
        String[] hosts = ConfigFactory.Instance().getProperty("etcd.hosts").split(",");
        Random random = new Random(System.nanoTime());
        String host = hosts[random.nextInt(hosts.length)];
        System.out.println(host);
        logger.info("Using etcd host: " + host);
        String port = ConfigFactory.Instance().getProperty("etcd.port");

        this.client = Client.builder().endpoints("http://" + host + ":" + port).build();

        /**
         * Issue #181:
         * Do not use shutdown hook to close the client.
         * for that Java can not guarantee the executing order of the hooks,
         * and etcd client may be used by other hooks.
         *
         * However, ensure that the other hooks close the etcd client.
         */
    }

    public static EtcdUtil Instance()
    {
        return instance;
    }

    /**
     * @return the singleton client instance.
     */
    public Client getClient()
    {
        return client;
    }

    /**
     * @return the watch client from the singleton client instance.
     */
    public Watch getWatchClient()
    {
        return client.getWatchClient();
    }

    /**
     * get key-value by key from etcd.
     * you should ensure that there is only one value for this key.
     *
     * @param key etcdKey
     * @return null if the key is not found.
     */
    public KeyValue getKeyValue(String key)
    {
        KeyValue keyValue = null;
        try
        {
            List<KeyValue> keyValues = this.client.getKVClient().get(
                    ByteSequence.from(key, StandardCharsets.UTF_8)).get().getKvs();
            if (keyValues.size() > 0)
            {
                keyValue = keyValues.get(0);
            }
        }
        catch (Exception e)
        {
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
    public List<KeyValue> getKeyValuesByPrefix(String prefix)
    {
        List<KeyValue> keyValues = new ArrayList<>();
        GetOption getOption = GetOption.newBuilder().withPrefix(
                ByteSequence.from(prefix, StandardCharsets.UTF_8)).build();
        try
        {
            keyValues = this.client.getKVClient().get(
                    ByteSequence.from(prefix, StandardCharsets.UTF_8), getOption).get().getKvs();
        }
        catch (Exception e)
        {
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
    public void putKeyValue(String key, String value)
    {
        CompletableFuture<PutResponse> future = client.getKVClient().put(
                ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value, StandardCharsets.UTF_8));
        try
        {
            future.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            logger.error("error when put key-value into etcd.", e);
        }
    }

    /**
     * put key-value into etcd.
     *
     * @param key
     * @param value
     */
    public void putKeyValue(String key, byte[] value)
    {
        CompletableFuture<PutResponse> future = client.getKVClient().put(
                ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value));
        try
        {
            future.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            logger.error("error when put key-value into etcd.", e);
        }
    }

    /**
     * put key-value into etcd with an expiry time (by etcd lease).
     *
     * @param key
     * @param value
     * @param expireTime expire time in seconds.
     * @return lease id, 0L if error occurs.
     */
    public long putKeyValueWithExpireTime(String key, String value, long expireTime)
    {
        CompletableFuture<LeaseGrantResponse> leaseGrantResponse = this.client.getLeaseClient().grant(expireTime);
        PutOption putOption;
        try
        {
            putOption = PutOption.newBuilder().withLeaseId(leaseGrantResponse.get().getID()).build();
            this.client.getKVClient().put(ByteSequence.from(key, StandardCharsets.UTF_8),
                    ByteSequence.from(value, StandardCharsets.UTF_8), putOption);
            return leaseGrantResponse.get().getID();
        }
        catch (Exception e)
        {
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
    public long putKeyValueWithLeaseId(String key, String value, long leaseId) throws Exception
    {
        PutOption putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
        CompletableFuture<PutResponse> putResponse = this.client.getKVClient().put(
                ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value, StandardCharsets.UTF_8), putOption);
        try
        {
            return putResponse.get().getHeader().getRevision();
        }
        catch (Exception e)
        {
            logger.error("error when put key-value with lease id into etcd.", e);
        }
        return 0L;
    }

    /**
     * delete key-value by key.
     *
     * @param key
     */
    public void delete(String key)
    {
        this.client.getKVClient().delete(ByteSequence.from(key, StandardCharsets.UTF_8));
    }

    /**
     * delete all key-values with this prefix.
     *
     * @param prefix
     */
    public void deleteByPrefix(String prefix)
    {
        DeleteOption deleteOption = DeleteOption.newBuilder().withPrefix(
                ByteSequence.from(prefix, StandardCharsets.UTF_8)).build();
        this.client.getKVClient().delete(ByteSequence.from(prefix, StandardCharsets.UTF_8), deleteOption);
    }
}