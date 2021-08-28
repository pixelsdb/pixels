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
package io.pixelsdb.pixels.daemon.cache;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.pixelsdb.pixels.cache.PixelsCacheConfig;
import io.pixelsdb.pixels.cache.PixelsCacheUtil;
import io.pixelsdb.pixels.cache.PixelsCacheWriter;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * pixels cache manager.
 *
 * @author guodong
 * @author hank
 */
public class CacheManager
        implements Server
{
    enum CacheNodeStatus
    {
        UNHEALTHY(-1), READY(0), UPDATING(1), OUT_OF_SIZE(2);

        int statusCode;
        CacheNodeStatus(int statusCode)
        {
            this.statusCode = statusCode;
        }
    }
    private static Logger logger = LogManager.getLogger(CacheManager.class);
    // cache status: unhealthy(-1), ready(0), updating(1), out_of_size(2)
    private static AtomicInteger cacheStatus = new AtomicInteger(0);

    private PixelsCacheWriter cacheWriter = null;
    private MetadataService metadataService = null;
    private CacheManagerRegister cacheManagerRegister;
    private final PixelsCacheConfig cacheConfig;
    private final EtcdUtil etcdUtil;
    private final ScheduledExecutorService scheduledExecutor;
    /**
     * The hostname of the node where this CacheManager is running.
     */
    private String hostName;
    private boolean initializeSuccess = false;
    private int localCacheVersion = 0;

    public CacheManager()
    {
        this.cacheConfig = new PixelsCacheConfig();
        this.etcdUtil = EtcdUtil.Instance();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.hostName = System.getenv("HOSTNAME");
        logger.debug("HostName from system env: " + hostName);
        if (hostName == null) {
            try {
                this.hostName = InetAddress.getLocalHost().getHostName();
                logger.debug("HostName from InetAddress: " + hostName);
            }
            catch (UnknownHostException e) {
                logger.debug("Hostname is null. Exit");
                return;
            }
        }
        logger.debug("HostName: " + hostName);
        initialize();
    }

    /**
     * Initialize CacheManager
     *
     * 1. check if cache file exists.
     *    if exists, check if existing cache version is the same as current cache version in etcd.
     *      if not, existing cache is out of date, goto step #2.
     * 2. else, update caches with latest layout in etcd/mysql.
     * 3. update the status of CacheManager in etcd
     * 4. start a scheduled thread to update node (CacheManager) status
     * 5. add a watcher to listen to changes of the cache version in etcd.
     *    if there is a new version, we need to update caches according to new layouts.
     * */
    private void initialize()
    {
        try {
            KeyValue cacheCoordinatorKV = etcdUtil.getKeyValue(Constants.CACHE_COORDINATOR_LITERAL);
            if (cacheCoordinatorKV == null) {
                logger.info("No coordinator found. Exit");
                return;
            }
            // init cache writer and metadata service
            this.cacheWriter =
                    PixelsCacheWriter.newBuilder()
                            .setCacheLocation(cacheConfig.getCacheLocation())
                            .setCacheSize(cacheConfig.getCacheSize())
                            .setIndexLocation(cacheConfig.getIndexLocation())
                            .setIndexSize(cacheConfig.getIndexSize())
                            .setOverwrite(false)
                            .setHostName(hostName)
                            .setCacheConfig(cacheConfig)
                            .build(); // cache version in the index file is cleared if its first 6 bytes are not magic ("PIXELS").
            this.metadataService = new MetadataService(cacheConfig.getMetaHost(), cacheConfig.getMetaPort());

            // Update cache if necessary.
            // If the cache is new created using start-vm.sh script, localCacheVersion would be zero.
            localCacheVersion = PixelsCacheUtil.getIndexVersion(cacheWriter.getIndexFile());
            logger.debug("Local cache version: " + localCacheVersion);
            // If Pixels has been reset by reset-pixels.sh, the cache version in etcd would be zero too.
            KeyValue globalCacheVersionKV = etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL);
            if (null != globalCacheVersionKV) {
                int globalCacheVersion = Integer.parseInt(globalCacheVersionKV.getValue().toString(StandardCharsets.UTF_8));
                logger.debug("Current global cache version: " + globalCacheVersion);
                // if cache file exists already. we need check local cache version with global cache version stored in etcd
                if (localCacheVersion >= 0 && localCacheVersion < globalCacheVersion) {
                    // If global version is ahead the local one, update local cache.
                    update(globalCacheVersion);
                }
            }
            // register a datanode
            Lease leaseClient = etcdUtil.getClient().getLeaseClient();
            long leaseId = leaseClient.grant(cacheConfig.getNodeLeaseTTL()).get(10, TimeUnit.SECONDS).getID();
            // start a scheduled thread to update node status periodically
            this.cacheManagerRegister = new CacheManagerRegister(leaseClient, leaseId);
            scheduledExecutor.scheduleAtFixedRate(cacheManagerRegister, 1, 10, TimeUnit.SECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
            initializeSuccess = true;
            etcdUtil.putKeyValue(Constants.CACHE_NODE_STATUS_LITERAL + hostName, "" + cacheStatus.get());
        }
        catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void update(int version)
            throws MetadataException
    {
        Layout matchedLayout = metadataService.getLayout(cacheConfig.getSchema(), cacheConfig.getTable(), version);
        if (matchedLayout != null) {
            // update cache status
            cacheStatus.set(CacheNodeStatus.UPDATING.statusCode);
            etcdUtil.putKeyValue(Constants.CACHE_NODE_STATUS_LITERAL + hostName, "" + cacheStatus.get());
            // update cache content
            int status = 0;
            if (cacheWriter.isCacheEmpty() == true)
            {
                status = cacheWriter.updateAll(version, matchedLayout);
            }
            else
            {
                status = cacheWriter.updateIncremental(version, matchedLayout);
            }
            cacheStatus.set(status);
            etcdUtil.putKeyValue(Constants.CACHE_NODE_STATUS_LITERAL + hostName, "" + cacheStatus.get());
            localCacheVersion = version;
        }
        else {
            logger.warn("No matching layout found for the update version " + version);
        }
    }

    @Override
    public void run()
    {
        logger.info("Starting cache manager");
        if (false == initializeSuccess) {
            logger.info("Initialization failed. Stop now.");
            return;
        }
        CountDownLatch latch = new CountDownLatch(1);
        Watch.Watcher watcher = etcdUtil.getClient().getWatchClient().watch(
                ByteSequence.from(Constants.CACHE_VERSION_LITERAL, StandardCharsets.UTF_8),
                WatchOption.DEFAULT, watchResponse ->
                {
                    for (WatchEvent event : watchResponse.getEvents())
                    {

                        if (event.getEventType() == WatchEvent.EventType.PUT)
                        {
                            // Get a new cache layout version.
                            int version = Integer.parseInt(event.getKeyValue().getValue().toString(StandardCharsets.UTF_8));
                            if (cacheStatus.get() == CacheNodeStatus.READY.statusCode)
                            {
                                // Ready to update the local cache.
                                logger.debug("Cache version update detected, new global version is (" + version + ").");
                                if (version > localCacheVersion)
                                {
                                    // The new version is newer, update the local cache.
                                    logger.debug("New global version is greater than the local version, update the local cache.");
                                    try
                                    {
                                        update(version);
                                    } catch (MetadataException e)
                                    {
                                        logger.error(e.getMessage());
                                        e.printStackTrace();
                                    }
                                }
                            }
                            else if (cacheStatus.get() == CacheNodeStatus.UPDATING.statusCode)
                            {
                                // The local cache is been updating, ignore the new version.
                                logger.warn("The local cache is been updating for version (" + localCacheVersion + "), " +
                                        "ignore the new cache version (" + version + ").");
                            }
                            else
                            {
                                // Shutdown this cache manager.
                                latch.countDown();
                            }
                        }
                        else if (event.getEventType() == WatchEvent.EventType.DELETE)
                        {
                            logger.warn("Cache version deletion detected, the cluster is corrupted. Stop now.");
                            cacheStatus.set(CacheNodeStatus.UNHEALTHY.statusCode);
                            // Shutdown the cache manager.
                            latch.countDown();
                        }
                    }
                });
        try
        {
            // Wait for this cache manager to be shutdown.
            latch.await();
        } catch (InterruptedException e)
        {
            logger.error(e.getMessage());
            e.printStackTrace();
        } finally
        {
            watcher.close();
        }
    }

    @Override
    public boolean isRunning()
    {
        return cacheStatus.get() >= 0;
    }

    @Override
    public void shutdown()
    {
        cacheStatus.set(CacheNodeStatus.UNHEALTHY.statusCode);
        cacheManagerRegister.stop();
        etcdUtil.delete(Constants.CACHE_NODE_STATUS_LITERAL + hostName);
        logger.info("CacheManager on " + hostName + " shut down.");
        this.scheduledExecutor.shutdownNow();
    }

    /**
     * Scheduled register to update caching node status and keep its registration alive.
     * */
    private static class CacheManagerRegister
            implements Runnable
    {
        private final Lease leaseClient;
        private final long leaseId;

        CacheManagerRegister(Lease leaseClient, long leaseId)
        {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;

        }

        @Override
        public void run()
        {
            try {
                leaseClient.keepAliveOnce(leaseId);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void stop()
        {
            leaseClient.revoke(leaseId);
            leaseClient.close();
        }
    }
}
