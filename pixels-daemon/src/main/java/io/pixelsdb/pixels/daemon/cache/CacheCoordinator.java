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

import com.google.common.collect.ImmutableList;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.pixelsdb.pixels.cache.CacheLocationDistribution;
import io.pixelsdb.pixels.cache.PixelsCacheConfig;
import io.pixelsdb.pixels.common.balance.AbsoluteBalancer;
import io.pixelsdb.pixels.common.balance.Balancer;
import io.pixelsdb.pixels.common.balance.ReplicaBalancer;
import io.pixelsdb.pixels.common.exception.BalancerException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.server.HostAddress;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.heartbeat.NodeStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * CacheCoordinator is responsible for the following tasks:
 * 1. caching balance. It assigns each file a caching location, which are updated into etcd for global synchronization, and maintains a dynamic caching balance in the cluster.
 * 3. caching node monitor. It monitors all cache workers in the cluster, and update running status(available, busy, dead, etc.) of caching nodes.
 *
 * @author guodong, hank
 */
// TODO: cache location compaction. Cache locations for older versions still exist after being used.
public class CacheCoordinator implements Server
{
    private static final Logger logger = LogManager.getLogger(CacheCoordinator.class);
    private final PixelsCacheConfig cacheConfig;
    private MetadataService metadataService = null;
    private Storage storage = null;
    private boolean initializeSuccess = false;
    private CountDownLatch runningLatch;
    private boolean running = false;

    public CacheCoordinator()
    {
        this.cacheConfig = new PixelsCacheConfig();
        initialize();
    }

    /**
     * Initialize cache coordinator:
     * <p>
     * 1. create the storage instance that is used to get the metadata of the storage;
     * 2. check the local and the global cache versions, update the cache plan if needed.
     * </p>
     */
    private void initialize()
    {
        try
        {
            if (cacheConfig.isCacheEnabled())
            {
                // 1. create the storage instance
                if (storage == null)
                {
                    storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());
                }
                // 2. check version consistency
                int cacheVersion = -1, layoutVersion = -1;
                SchemaTableName schemaTableName = null;
                this.metadataService = MetadataService.Instance();
                KeyValue cacheVersionKV = EtcdUtil.Instance().getKeyValue(Constants.CACHE_VERSION_LITERAL);
                if (null != cacheVersionKV)
                {
                    cacheVersion = Integer.parseInt(cacheVersionKV.getValue().toString(StandardCharsets.UTF_8));
                }
                KeyValue layoutVersionKV = EtcdUtil.Instance().getKeyValue(Constants.LAYOUT_VERSION_LITERAL);
                if (layoutVersionKV != null)
                {
                    // Issue #636: get schema and table name from etcd instead of config file.
                    String value = layoutVersionKV.getValue().toString(StandardCharsets.UTF_8);
                    String[] splits = value.split(":");
                    checkArgument(splits.length == 2, "invalid value for key '" +
                            Constants.LAYOUT_VERSION_LITERAL + "' in etcd: " + value);
                    schemaTableName = new SchemaTableName(splits[0]);
                    layoutVersion = Integer.parseInt(splits[1]);
                    checkArgument(layoutVersion >= 0,
                            "invalid layout version in etcd: " + layoutVersion);
                }
                if (cacheVersion >= 0 && layoutVersion >= 0 && cacheVersion < layoutVersion)
                {
                    logger.debug("Current cache version is left behind of current layout version, update the cache...");
                    updateCachePlan(schemaTableName, layoutVersion);
                }

                logger.info("Cache coordinator on is initialized");
            }
            else
            {
                logger.info("Cache is disabled, nothing to initialize");
            }
            initializeSuccess = true;
        } catch (Exception e)
        {
            logger.error("failed to initialize cache coordinator", e);
        }
    }

    @Override
    public void run()
    {
        if (!initializeSuccess)
        {
            logger.error("Cache coordinator initialization failed, exit now...");
            return;
        }

        if (!cacheConfig.isCacheEnabled())
        {
            logger.info("Cache is disabled, exit...");
            return;
        }

        synchronized (this) 
        {
            if (this.running)
            {
                return;
            } 
            else 
            {
                this.running = true;
            }
        }
        
        logger.info("Starting cache coordinator");
        runningLatch = new CountDownLatch(1);
        // watch layout version change, and update the cache plan and the local cache version
        Watch.Watcher watcher = EtcdUtil.Instance().getClient().getWatchClient().watch(
                ByteSequence.from(Constants.LAYOUT_VERSION_LITERAL, StandardCharsets.UTF_8),
                WatchOption.DEFAULT, watchResponse ->
                {
                    for (WatchEvent event : watchResponse.getEvents())
                    {
                        if (event.getEventType() == WatchEvent.EventType.PUT)
                        {
                            // listen to the PUT even on the LAYOUT VERSION that can be changed by rainbow.
                            try
                            {
                                // this coordinator is ready.
                                logger.debug("Update cache distribution");
                                // update the cache distribution
                                String value = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                                // Issue #636: get schema and table name from etcd instead of config file.
                                String[] splits = value.split(":");
                                checkArgument(splits.length == 2, "Invalid value for key '" +
                                        Constants.LAYOUT_VERSION_LITERAL + "' in etcd: " + value);
                                SchemaTableName schemaTableName = new SchemaTableName(splits[0]);
                                int layoutVersion = Integer.parseInt(splits[1]);
                                checkArgument(layoutVersion >= 0,
                                        "Invalid layout version in etcd: " + layoutVersion);
                                updateCachePlan(schemaTableName, layoutVersion);
                                // update cache version, notify cache managers on each node to update cache.
                                logger.debug("Update cache version to {}", layoutVersion);
                                EtcdUtil.Instance().putKeyValue(Constants.CACHE_VERSION_LITERAL, value);
                            }
                            catch (IOException | MetadataException e)
                            {
                                logger.error("Failed to update cache distribution", e);
                            }
                            break;
                        }
                    }
                });
        try
        {
            // Wait for this coordinator to be shutdown
            logger.info("Cache coordinator is running");
            runningLatch.await();
        }
        catch (InterruptedException e)
        {
            logger.error("Cache coordinator interrupted when waiting on the running latch", e);
        }
        finally
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
        return this.running;
    }

    @Override
    public void shutdown()
    {
        this.running = false;
        logger.debug("Shutting down cache coordinator...");
        if (metadataService != null)
        {
            // Issue #708: no need the shut down the metadata service instance created using the configured host and port.
            metadataService = null;
        }
        // No need to close the storage instance as it is managed by the StorageFactory.
        if (runningLatch != null)
        {
            runningLatch.countDown();
        }
        EtcdUtil.Instance().getClient().close();
        logger.info("Cache coordinator is shutdown");
    }

    /**
     * Update file caching locations:
     * 1. for all files, decide which files to cache
     * 2. for each file, decide which node to cache it
     * */
    private void updateCachePlan(SchemaTableName schemaTableName, int layoutVersion)
            throws MetadataException, IOException
    {
        Layout layout = metadataService.getLayout(
                schemaTableName.getSchemaName(), schemaTableName.getTableName(), layoutVersion);
        // select: decide which files to cache
        assert layout != null;
        List<String> filePaths = select(layout);
        // allocate: decide which node to cache each file
        List<KeyValue> workerNodes = EtcdUtil.Instance().getKeyValuesByPrefix(Constants.HEARTBEAT_WORKER_LITERAL);
        if (workerNodes == null || workerNodes.isEmpty())
        {
            logger.info("Nodes is null or empty, no updates");
            return;
        }
        HostAddress[] workerHosts = new HostAddress[workerNodes.size()];
        int hostIndex = 0;
        for (KeyValue node : workerNodes)
        {
            // key: host_[hostname]; value: [status]. available if status == 1.
            if (Integer.parseInt(node.getValue().toString(StandardCharsets.UTF_8)) ==
                    NodeStatus.READY.StatusCode)
            {
                workerHosts[hostIndex++] = HostAddress.fromString(node.getKey()
                        .toString(StandardCharsets.UTF_8).substring(Constants.HEARTBEAT_WORKER_LITERAL.length()));
            }
        }
        allocate(filePaths, workerHosts, hostIndex, layoutVersion);
    }

    /**
     * Get the file paths under the compact path of the storage layout to be cached.
     * @param layout the storage layout to be cached
     * @return the file paths
     * @throws MetadataException if fails to get file paths under the layout
     */
    private List<String> select(Layout layout) throws MetadataException
    {
        List<Path> compactPaths = layout.getCompactPaths();
        List<String> filePaths = new LinkedList<>();
        // Issue #723: files are managed in metadata, do not get file paths from storage.
        for (Path compactPath : compactPaths)
        {
            this.metadataService.getFiles(compactPath.getId()).forEach(
                    file -> filePaths.add(File.getFilePath(compactPath, file)));
        }
        return filePaths;
    }

    /**
     * Allocate (map) file paths to worker nodes, and persists the result (cache tasks) in etcd.
     * @param filePaths the paths of the files to cache
     * @param workers the hostnames of the worker nodes
     * @param size the number of worker to allocate the cache tasks
     * @param layoutVersion the version of the new layout to be cached
     * @throws IOException if fails to allocate cache locations
     */
    private void allocate(List<String> filePaths, HostAddress[] workers, int size, int layoutVersion) throws IOException
    {
        CacheLocationDistribution cacheLocationDistribution = assignCacheLocations(filePaths, workers, size);
        for (int i = 0; i < size; i++)
        {
            HostAddress node = workers[i];
            Set<String> files = cacheLocationDistribution.getCacheDistributionByLocation(node.toString());
            String key = Constants.CACHE_LOCATION_LITERAL + layoutVersion + "_" + node;
            logger.debug("{} files are allocated to {} at layout version{}", files.size(), node, layoutVersion);
            EtcdUtil.Instance().putKeyValue(key, String.join(";", files));
        }
    }

    /**
     * assign files / objects to cache manager nodes randomly, guaranty load balance.
     * @param paths
     * @param nodes
     * @param size
     * @return
     * @throws IOException
     */
    private CacheLocationDistribution assignCacheLocations(List<String> paths, HostAddress[] nodes, int size) throws IOException
    {
        CacheLocationDistribution locationDistribution = new CacheLocationDistribution(nodes, size);

        List<HostAddress> cacheNodes = new ArrayList<>();
        for (int i = 0; i < size; i++)
        {
            cacheNodes.add(nodes[i]);
        }

        Balancer replicaBalancer = new ReplicaBalancer(cacheNodes);
        for (String path : paths)
        {
            if (storage.hasLocality())
            {
                // get a set of nodes where the blocks of the file is located (location_set)
                Set<HostAddress> addresses = new HashSet<>();
                List<Location> locations = storage.getLocations(path);
                for (Location location : locations)
                {
                    addresses.addAll(toHostAddress(location.getHosts()));
                }
                // addresses should not be empty.
                replicaBalancer.put(path, addresses);
            }
            else
            {
                replicaBalancer.autoSelect(path);
            }
        }
        try
        {
            replicaBalancer.balance();
            if (replicaBalancer.isBalanced())
            {
                boolean enableAbsolute = Boolean.parseBoolean(
                        ConfigFactory.Instance().getProperty("cache.absolute.balancer.enabled"));
                if (enableAbsolute)
                {
                    Balancer absoluteBalancer = new AbsoluteBalancer();
                    replicaBalancer.cascade(absoluteBalancer);
                    absoluteBalancer.balance();
                    if (absoluteBalancer.isBalanced())
                    {
                        Map<String, HostAddress> balanced = absoluteBalancer.getAll();
                        for (Map.Entry<String, HostAddress> entry : balanced.entrySet())
                        {
                            String host = entry.getValue().toString();
                            String path = entry.getKey();
                            locationDistribution.addCacheLocation(host, path);
                        }
                    }
                    else
                    {
                        throw new BalancerException("absolute balancer failed to balance paths");
                    }
                }
                else
                {
                    Map<String, HostAddress> balanced = replicaBalancer.getAll();
                    for (Map.Entry<String, HostAddress> entry : balanced.entrySet())
                    {
                        String host = entry.getValue().toString();
                        String path = entry.getKey();
                        locationDistribution.addCacheLocation(host, path);
                    }
                }
            }
            else
            {
                throw new BalancerException("replica balancer failed to balance paths");
            }
        } catch (BalancerException e)
        {
            logger.error("failed to balance cache distribution", e);
        }

        return locationDistribution;
    }

    private List<HostAddress> toHostAddress(String[] hosts)
    {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts)
        {
            builder.add(HostAddress.fromString(host));
        }
        return builder.build();
    }
}
