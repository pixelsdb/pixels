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

        public final int StatusCode;
        CacheNodeStatus(int statusCode)
        {
            this.StatusCode = statusCode;
        }
    }
    private static Logger logger = LogManager.getLogger(CacheManager.class);
    // cache status: unhealthy(-1), ready(0), updating(1), out_of_size(2)
    private static AtomicInteger cacheStatus = new AtomicInteger(CacheNodeStatus.READY.StatusCode);

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
    private CountDownLatch runningLatch;

    public CacheManager()
    {
        this.cacheConfig = new PixelsCacheConfig();
        this.etcdUtil = EtcdUtil.Instance();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.hostName = System.getenv("HOSTNAME");
        logger.debug("HostName from system env: " + hostName);
        if (hostName == null)
        {
            try
            {
                this.hostName = InetAddress.getLocalHost().getHostName();
                logger.debug("HostName from InetAddress: " + hostName);
            } catch (UnknownHostException e)
            {
                logger.debug("Hostname is null. Exit");
                return;
            }
        }
        logger.debug("HostName: " + hostName);
        initialize();
    }

    /**
     * Initialize CacheManager:
     *
     * 1. check if the cache coordinator exists.
     *      if not, initialization is failed and return.
     * 2. initialize the metadata service and cache writer.
     * 3. check if local cache version is the same as global cache version in etcd.
     *      if the local cache is not up-to-date, update the local cache.
     * 4. register the DataNode by updating the status of CacheManager in etcd.
     *
     * <p>
     *     2 and 3 are only executed if the cache is enabled.
     * </p>
     * <p>
     *     A watcher will be added in run() to listen to changes of the cache version in etcd.
     *     if there is a new version, we need to update caches according to new layouts.
     * </p>
     * */
    private void initialize()
    {
        try
        {
            // 1. check the existence of the cache coordinator.
            KeyValue cacheCoordinatorKV = etcdUtil.getKeyValue(Constants.CACHE_COORDINATOR_LITERAL);
            if (cacheCoordinatorKV == null)
            {
                logger.error("No coordinator found. Exit");
                return;
            }
            if (cacheConfig.isCacheEnabled())
            {
                // 2. init cache writer and metadata service
                this.cacheWriter = PixelsCacheWriter.newBuilder()
                        .setCacheLocation(cacheConfig.getCacheLocation())
                        .setCacheSize(cacheConfig.getCacheSize())
                        .setIndexLocation(cacheConfig.getIndexLocation())
                        .setIndexSize(cacheConfig.getIndexSize())
                        .setOverwrite(false)
                        .setHostName(hostName)
                        .setCacheConfig(cacheConfig)
                        .build(); // cache version in the index file is cleared if its first 6 bytes are not magic ("PIXELS").
                this.metadataService = new MetadataService(cacheConfig.getMetaHost(), cacheConfig.getMetaPort());

                // 3. Update cache if necessary.
                // If the cache is new created using start-vm.sh script, the local cache version would be zero.
                localCacheVersion = PixelsCacheUtil.getIndexVersion(cacheWriter.getIndexFile());
                logger.debug("Local cache version: " + localCacheVersion);
                // If Pixels has been reset by reset-pixels.sh, the cache version in etcd would be zero too.
                KeyValue globalCacheVersionKV = etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL);
                if (globalCacheVersionKV != null)
                {
                    int globalCacheVersion = Integer.parseInt(globalCacheVersionKV.getValue().toString(StandardCharsets.UTF_8));
                    logger.debug("Current global cache version: " + globalCacheVersion);
                    // if cache file exists already. we need check local cache version with global cache version stored in etcd
                    if (localCacheVersion >= 0 && localCacheVersion < globalCacheVersion)
                    {
                        // If global version is ahead the local one, update local cache.
                        updateLocalCache(globalCacheVersion);
                    }
                }
            }
            // 4. register the datanode
            Lease leaseClient = etcdUtil.getClient().getLeaseClient();
            long leaseId = leaseClient.grant(cacheConfig.getNodeLeaseTTL()).get(10, TimeUnit.SECONDS).getID();
            // start a scheduled thread to update node status periodically
            this.cacheManagerRegister = new CacheManagerRegister(leaseClient, leaseId);
            scheduledExecutor.scheduleAtFixedRate(cacheManagerRegister,
                    0, cacheConfig.getNodeHeartbeatPeriod(), TimeUnit.SECONDS);
            initializeSuccess = true;
            etcdUtil.putKeyValue(Constants.CACHE_NODE_STATUS_LITERAL + hostName, "" + cacheStatus.get());
        } catch (Exception e)
        {
            logger.error("failed to initialize cache manager", e);
        }
    }

    private void updateLocalCache(int version)
            throws MetadataException
    {
        Layout matchedLayout = metadataService.getLayout(cacheConfig.getSchema(), cacheConfig.getTable(), version);
        if (matchedLayout != null)
        {
            // update cache status
            cacheStatus.set(CacheNodeStatus.UPDATING.StatusCode);
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
        } else
        {
            logger.warn("No matching layout found for the update version " + version);
        }
    }

    @Override
    public void run()
    {
        logger.info("Starting Node Manager");
        if (!initializeSuccess)
        {
            logger.error("Initialization failed, stop now...");
            return;
        }
        runningLatch = new CountDownLatch(1);
        Watch.Watcher watcher = null;
        if (cacheConfig.isCacheEnabled())
        {
            watcher = etcdUtil.getClient().getWatchClient().watch(
                    ByteSequence.from(Constants.CACHE_VERSION_LITERAL, StandardCharsets.UTF_8),
                    WatchOption.DEFAULT, watchResponse ->
                    {
                        for (WatchEvent event : watchResponse.getEvents())
                        {
                            if (event.getEventType() == WatchEvent.EventType.PUT)
                            {
                                // Get a new cache layout version.
                                int version = Integer.parseInt(event.getKeyValue().getValue().toString(StandardCharsets.UTF_8));
                                if (cacheStatus.get() == CacheNodeStatus.READY.StatusCode)
                                {
                                    // Ready to update the local cache.
                                    logger.debug("Cache version update detected, new global version is (" + version + ").");
                                    if (version > localCacheVersion)
                                    {
                                        // The new version is newer, update the local cache.
                                        logger.debug("New global version is greater than the local version, update the local cache.");
                                        try
                                        {
                                            updateLocalCache(version);
                                        } catch (MetadataException e)
                                        {
                                            logger.error("Failed to update local cache.", e);
                                            e.printStackTrace();
                                        }
                                    }
                                } else if (cacheStatus.get() == CacheNodeStatus.UPDATING.StatusCode)
                                {
                                    // The local cache is updating, ignore the new version.
                                    logger.warn("The local cache is updating for version (" + localCacheVersion + "), " +
                                            "ignore the new cache version (" + version + ").");
                                } else
                                {
                                    // Shutdown this node manager.
                                    runningLatch.countDown();
                                }
                            } else if (event.getEventType() == WatchEvent.EventType.DELETE)
                            {
                                logger.warn("Cache version deletion detected, the cluster is corrupted. Stop now.");
                                cacheStatus.set(CacheNodeStatus.UNHEALTHY.StatusCode);
                                // Shutdown the node manager.
                                runningLatch.countDown();
                            }
                        }
                    });
        }
        try
        {
            // Wait for this cache manager to be shutdown.
            runningLatch.await();
        } catch (InterruptedException e)
        {
            logger.error("Node Manager was interrupted abnormally.", e);
        } finally
        {
            if (watcher != null)
            {
                watcher.close();
            }
        }
    }

    @Override
    public boolean isRunning()
    {
        int status = cacheStatus.get();
        return status == CacheNodeStatus.READY.StatusCode ||
                status == CacheNodeStatus.UPDATING.StatusCode;
    }

    @Override
    public void shutdown()
    {
        cacheStatus.set(CacheNodeStatus.UNHEALTHY.StatusCode);
        logger.info("Shutting down Node Manager...");
        scheduledExecutor.shutdownNow();
        if (cacheManagerRegister != null)
        {
            cacheManagerRegister.stop();
        }
        etcdUtil.delete(Constants.CACHE_NODE_STATUS_LITERAL + hostName);
        if (metadataService != null)
        {
            try
            {
                metadataService.shutdown();
            } catch (InterruptedException e)
            {
                logger.error("Failed to shutdown rpc channel for metadata service " +
                        "while shutting down Node Manager.", e);
            }
        }
        if (cacheWriter != null)
        {
            try
            {
                cacheWriter.close();
            } catch (Exception e)
            {
                logger.error("Failed to close cache writer while shutting down Node Manager.", e);
            }
        }
        if (runningLatch != null)
        {
            runningLatch.countDown();
        }
        EtcdUtil.Instance().getClient().close();
        logger.info("Node Manager on '" + hostName + "' is shutdown.");
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
            try
            {
                leaseClient.keepAliveOnce(leaseId);
            } catch (Exception e)
            {
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
