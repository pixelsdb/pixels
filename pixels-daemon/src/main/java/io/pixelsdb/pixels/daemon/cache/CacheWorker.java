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
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.pixelsdb.pixels.cache.CacheScalingOperation;
import io.pixelsdb.pixels.cache.PixelsCacheConfig;
import io.pixelsdb.pixels.cache.PixelsCacheUtil;
import io.pixelsdb.pixels.cache.PixelsCacheWriter;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Pixels cache manager on each worker node.
 *
 * @author guodong, hank
 */
public class CacheWorker implements Server
{
    private static final Logger logger = LogManager.getLogger(CacheWorker.class);
    // cache status: unhealthy(-1), ready(0), updating(1), out_of_size(2)
    private static final AtomicInteger cacheStatus = new AtomicInteger(CacheWorkerStatus.READY.StatusCode);
    private PixelsCacheWriter cacheWriter = null;
    private MetadataService metadataService = null;
    private final PixelsCacheConfig cacheConfig;
    /**
     * The hostname of the node where this cache worker is running.
     */
    private String hostName;
    private boolean initializeSuccess = false;
    private int localCacheVersion = -1;
    private CountDownLatch runningLatch;

    public CacheWorker()
    {
        this.cacheConfig = new PixelsCacheConfig();
        this.hostName = System.getenv("HOSTNAME");
        logger.debug("HostName from system env: {}", hostName);
        if (hostName == null)
        {
            try
            {
                this.hostName = InetAddress.getLocalHost().getHostName();
                logger.debug("HostName from InetAddress: {}", hostName);
            } catch (UnknownHostException e)
            {
                logger.debug("Hostname is null. Exit");
                return;
            }
        }
        logger.debug("HostName: {}", hostName);
        initialize();
    }

    /**
     * Initialize cache worker:
     * <p>
     * 1. initialize the metadata service and cache writer;
     * 2. check if local cache version is the same as global cache version in etcd.
     * If the local cache is not up-to-date, update the local cache.
     * </p>
     * */
    private void initialize()
    {
        try
        {
            if (cacheConfig.isCacheEnabled())
            {
                // 1. init cache writer and metadata service.
                this.cacheWriter = PixelsCacheWriter.newBuilder()
                        .setCacheBaseLocation(cacheConfig.getCacheLocation())
                        .setCacheSize(cacheConfig.getCacheSize())
                        .setIndexBaseLocation(cacheConfig.getIndexLocation())
                        .setIndexSize(cacheConfig.getIndexSize())
                        .setZoneNum(cacheConfig.getZoneNum(), cacheConfig.getSwapZoneNum())
                        .setOverwrite(false)
                        .setHostName(hostName)
                        .setCachedStorage(StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme()))
                        .build(); // cache version in the index file is cleared if its first 6 bytes are not magic ("PIXELS").
                this.metadataService = MetadataService.Instance();

                // 2. update cache if necessary.
                // If the cache is new created using start-vm.sh script, the local cache version would be zero.
                localCacheVersion = PixelsCacheUtil.getIndexVersion(cacheWriter.getIndexFile(), cacheWriter);
                // If Pixels has been reset by reset-pixels.sh, the cache version in etcd would be zero too.
                KeyValue globalCacheVersionKV = EtcdUtil.Instance().getKeyValue(Constants.CACHE_VERSION_LITERAL);
                if (globalCacheVersionKV != null)
                {
                    // Issue #636: get schema and table name from etcd instead of config file.
                    String value = globalCacheVersionKV.getValue().toString(StandardCharsets.UTF_8);
                    String[] splits = value.split(":");
                    checkArgument(splits.length == 2, "invalid value for key '" +
                            Constants.CACHE_VERSION_LITERAL + "' in etcd: " + value);
                    SchemaTableName schemaTableName = new SchemaTableName(splits[0]);
                    int globalCacheVersion = Integer.parseInt(splits[1]);
                    checkArgument(globalCacheVersion >= 0,
                            "invalid cache version in etcd: " + globalCacheVersion);
                    logger.debug("Current global cache version: {}", globalCacheVersion);
                    // if cache file exists already. we need check local cache version with global cache version stored in etcd.
                    if (localCacheVersion >= 0 && localCacheVersion < globalCacheVersion)
                    {
                        // If global version is ahead the local one, update local cache.
                        updateLocalCache(schemaTableName, globalCacheVersion);
                    }
                }
            }
            else
            {
                logger.info("Cache is disabled, nothing to initialize");
            }
            initializeSuccess = true;
        } catch (Exception e)
        {
            logger.error("failed to initialize cache worker", e);
        }
    }

    private void updateLocalCache(SchemaTableName schemaTableName, int version) throws MetadataException
    {
        Layout matchedLayout = metadataService.getLayout(
                schemaTableName.getSchemaName(), schemaTableName.getTableName(), version);
        if (matchedLayout != null)
        {
            // update cache status
            cacheStatus.set(CacheWorkerStatus.UPDATING.StatusCode);
            // update cache content
            int status;
            if (cacheWriter.isCacheEmpty())
            {
                logger.debug("Cache update all for layout version {}", version);
                status = cacheWriter.updateAll(version, matchedLayout);
            }
            else
            {
                logger.debug("Cache update incremental for layout version {}", version);
                status = cacheWriter.updateIncremental(version, matchedLayout);
            }
            cacheStatus.set(status);
            localCacheVersion = version;
        } else
        {
            logger.warn("No matching layout found for the update version {}", version);
        }
    }

    @Override
    public void run()
    {
        if (!initializeSuccess)
        {
            logger.error("Cache worker initialization failed, exit...");
            return;
        }

        if (!cacheConfig.isCacheEnabled())
        {
            logger.info("Cache is disabled, exit...");
            return;
        }

        logger.info("Starting cache worker");
        runningLatch = new CountDownLatch(1);
        Watch.Watcher watcher = EtcdUtil.Instance().getClient().getWatchClient().watch(
                ByteSequence.from(Constants.CACHE_VERSION_LITERAL, StandardCharsets.UTF_8),
                WatchOption.DEFAULT, watchResponse ->
                {
                    for (WatchEvent event : watchResponse.getEvents())
                    {
                        if (event.getEventType() == WatchEvent.EventType.PUT)
                        {
                            // Get a new cache layout version.
                            if (cacheStatus.get() == CacheWorkerStatus.READY.StatusCode)
                            {
                                // Ready to update the local cache.
                                String value = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                                String[] splits = value.split(":");
                                checkArgument(splits.length == 2, "invalid value for key '" +
                                        Constants.CACHE_VERSION_LITERAL + "' in etcd: " + value);
                                SchemaTableName schemaTableName = new SchemaTableName(splits[0]);
                                int globalCacheVersion = Integer.parseInt(splits[1]);
                                checkArgument(globalCacheVersion >= 0,
                                        "invalid cache version in etcd: " + globalCacheVersion);
                                logger.debug("Cache version update detected, new global version is ({}).", globalCacheVersion);
                                if (globalCacheVersion > localCacheVersion)
                                {
                                    // The new version is newer, update the local cache.
                                    logger.debug("New global cache version {} is greater than the local version {}, update the local cache.",
                                            globalCacheVersion, localCacheVersion);
                                    try
                                    {
                                        updateLocalCache(schemaTableName, globalCacheVersion);
                                    } catch (MetadataException e)
                                    {
                                        logger.error("Failed to update local cache.", e);
                                    }
                                }
                                else
                                {
                                    logger.debug("New global cache version {} is equal to or less than the local version {}",
                                            globalCacheVersion, localCacheVersion);
                                }
                            } else if (cacheStatus.get() == CacheWorkerStatus.UPDATING.StatusCode)
                            {
                                // The local cache is updating, ignore the new version.
                                String value = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                                logger.warn("The local cache is updating, ignore the global cache version ({}).", value);
                            } else
                            {
                                // Shutdown this node manager.
                                runningLatch.countDown();
                            }
                        } else if (event.getEventType() == WatchEvent.EventType.DELETE)
                        {
                            logger.warn("Cache version deletion detected, the cluster is corrupted, exit now.");
                            cacheStatus.set(CacheWorkerStatus.UNHEALTHY.StatusCode);
                            // Shutdown the node manager.
                            runningLatch.countDown();
                        }
                    }
                });
        // watcher for cache expand or shrink.
        Watch.Watcher cacheScalingWatcher = EtcdUtil.Instance().getClient().getWatchClient().watch(
                ByteSequence.from(Constants.CACHE_EXPAND_OR_SHRINK_LITERAL, StandardCharsets.UTF_8),
                WatchOption.DEFAULT, watchResponse ->
                {
                    for (WatchEvent event : watchResponse.getEvents())
                    {
                        if (event.getEventType() == WatchEvent.EventType.PUT)
                        {
                            if (cacheStatus.get() == CacheWorkerStatus.READY.StatusCode)
                            {
                                String value = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                                int cacheScalingOperation = Integer.parseInt(value);
                                if (cacheScalingOperation == CacheScalingOperation.EXPAND.Operation)
                                {
                                    try {
                                        cacheWriter.expand();
                                    } catch (Exception e) {
                                        logger.error("Failed to expand cache.", e);
                                    }
                                } else if (cacheScalingOperation == CacheScalingOperation.SHRINK.Operation)
                                {
                                    try {
                                        cacheWriter.shrink();
                                    } catch (Exception e) {
                                        logger.error("Failed to shrink cache.", e);
                                    }
                                }
                            } else if (cacheStatus.get() == CacheWorkerStatus.UPDATING.StatusCode)
                            {
                                // The local cache is updating, ignore expand or shrink.
                                logger.warn("The local cache is updating, ignore the expand event.");
                            } else {
                                logger.warn("The local cache is not ready, ignore the expand event.");
                            }
                        }
                    }
                });
        try
        {
            // Wait for this cache worker to be shutdown.
            logger.info("Cache worker is running");
            runningLatch.await();
        } catch (InterruptedException e)
        {
            logger.error("Cache worker interrupted when waiting on the running latch", e);
        } finally
        {
            if (watcher != null)
            {
                watcher.close();
            }
            if (cacheScalingWatcher != null)
            {
                cacheScalingWatcher.close();
            }
        }
    }

    @Override
    public boolean isRunning()
    {
        int status = cacheStatus.get();
        return status == CacheWorkerStatus.READY.StatusCode ||
                status == CacheWorkerStatus.UPDATING.StatusCode;
    }

    @Override
    public void shutdown()
    {
        cacheStatus.set(CacheWorkerStatus.UNHEALTHY.StatusCode);
        logger.info("Shutting down cache worker...");
        if (metadataService != null)
        {
            // Issue #708: no need the shut down the metadata service instance created using the configured host and port.
            metadataService = null;
        }
        if (cacheWriter != null)
        {
            try
            {
                cacheWriter.close();
            } catch (Exception e)
            {
                logger.error("Failed to close cache writer while shutting down cache worker.", e);
            }
        }
        if (runningLatch != null)
        {
            runningLatch.countDown();
        }
        EtcdUtil.Instance().getClient().close();
        logger.info("Cache worker on '{}' is shutdown.", hostName);
    }
}
