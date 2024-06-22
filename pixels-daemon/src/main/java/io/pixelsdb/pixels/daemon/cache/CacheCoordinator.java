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
import io.pixelsdb.pixels.common.balance.HostAddress;
import io.pixelsdb.pixels.common.balance.ReplicaBalancer;
import io.pixelsdb.pixels.common.exception.BalancerException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
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

/**
 * CacheCoordinator is responsible for the following tasks:
 * 1. caching balance. It assigns each file a caching location, which are updated into etcd for global synchronization, and maintains a dynamic caching balance in the cluster.
 * 3. caching node monitor. It monitors all cache workers in the cluster, and update running status(available, busy, dead, etc.) of caching nodes.
 *
 * @author guodong
 * @author hank
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
                int cacheVersion = 0, layoutVersion = 0;
                this.metadataService = new MetadataService(cacheConfig.getMetaHost(), cacheConfig.getMetaPort());
                KeyValue cacheVersionKV = EtcdUtil.Instance().getKeyValue(Constants.CACHE_VERSION_LITERAL);
                if (null != cacheVersionKV)
                {
                    cacheVersion = Integer.parseInt(cacheVersionKV.getValue().toString(StandardCharsets.UTF_8));
                }
                KeyValue layoutVersionKV = EtcdUtil.Instance().getKeyValue(Constants.LAYOUT_VERSION_LITERAL);
                if (null != layoutVersionKV)
                {
                    layoutVersion = Integer.parseInt(layoutVersionKV.getValue().toString(StandardCharsets.UTF_8));
                }
                if (cacheVersion < layoutVersion)
                {
                    logger.debug("Current cache version is left behind of current layout version, update the cache...");
                    updateCachePlan(layoutVersion);
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
                                int layoutVersion = Integer.parseInt(event.getKeyValue().getValue().toString(StandardCharsets.UTF_8));
                                updateCachePlan(layoutVersion);
                                // update cache version, notify cache managers on each node to update cache.
                                logger.debug("Update cache version to " + layoutVersion);
                                EtcdUtil.Instance().putKeyValue(Constants.CACHE_VERSION_LITERAL, String.valueOf(layoutVersion));
                            } catch (IOException | MetadataException e)
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
        } catch (InterruptedException e)
        {
            logger.error("Cache coordinator interrupted when waiting on the running latch", e);
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
        return this.runningLatch.getCount() > 0;
    }

    @Override
    public void shutdown()
    {
        logger.debug("Shutting down cache coordinator...");
        if (metadataService != null)
        {
            try
            {
                metadataService.shutdown();
            } catch (InterruptedException e)
            {
                logger.error("Failed to shutdown rpc channel for metadata service " +
                        "while shutting down cache coordinator.", e);
            }
        }
        if (storage != null)
        {
            try
            {
                storage.close();
            } catch (IOException e)
            {
                logger.error("Failed to close cache storage while shutting down cache coordinator.", e);
            }
        }
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
    private void updateCachePlan(int layoutVersion) throws MetadataException, IOException
    {
        Layout layout = metadataService.getLayout(cacheConfig.getSchema(), cacheConfig.getTable(), layoutVersion);
        // select: decide which files to cache
        assert layout != null;
        String[] paths = select(layout);
        // allocate: decide which node to cache each file
        List<KeyValue> nodes = EtcdUtil.Instance().getKeyValuesByPrefix(Constants.HEARTBEAT_WORKER_LITERAL);
        if (nodes == null || nodes.isEmpty())
        {
            logger.info("Nodes is null or empty, no updates");
            return;
        }
        HostAddress[] hosts = new HostAddress[nodes.size()];
        int hostIndex = 0;
        for (int i = 0; i < nodes.size(); i++)
        {
            KeyValue node = nodes.get(i);
            // key: host_[hostname]; value: [status]. available if status == 1.
            if (Integer.parseInt(node.getValue().toString(StandardCharsets.UTF_8)) ==
                    NodeStatus.READY.StatusCode)
            {
                hosts[hostIndex++] = HostAddress.fromString(node.getKey()
                        .toString(StandardCharsets.UTF_8).substring(5));
            }
        }
        allocate(paths, hosts, hostIndex, layoutVersion);
    }

    /**
     * get the file paths under the compact path of the first layout.
     * @param layout
     * @return the file paths
     * @throws IOException
     */
    private String[] select(Layout layout) throws IOException
    {
        String[] compactPaths = layout.getCompactPathUris();
        List<String> files = new ArrayList<>();
        List<Status> statuses = storage.listStatus(compactPaths);
        if (statuses != null)
        {
            for (Status status : statuses)
            {
                if (status.isFile())
                {
                    files.add(status.getPath());
                }
            }
        }
        String[] result = new String[files.size()];
        files.toArray(result);
        return result;
    }

    /**
     * allocate (maps) file paths to nodes, and persists the result in etcd.
     * @param paths
     * @param nodes
     * @param size
     * @param layoutVersion
     * @throws IOException
     */
    private void allocate(String[] paths, HostAddress[] nodes, int size, int layoutVersion) throws IOException
    {
        CacheLocationDistribution cacheLocationDistribution = assignCacheLocations(paths, nodes, size);
        for (int i = 0; i < size; i++)
        {
            HostAddress node = nodes[i];
            Set<String> files = cacheLocationDistribution.getCacheDistributionByLocation(node.toString());
            String key = Constants.CACHE_LOCATION_LITERAL + layoutVersion + "_" + node;
            logger.debug(files.size() + " files are allocated to " + node + " at version" + layoutVersion);
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
    private CacheLocationDistribution assignCacheLocations(String[] paths, HostAddress[] nodes, int size) throws IOException
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
                        ConfigFactory.Instance().getProperty("enable.absolute.balancer"));
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
                    } else
                    {
                        throw new BalancerException("absolute balancer failed to balance paths");
                    }
                } else
                {
                    Map<String, HostAddress> balanced = replicaBalancer.getAll();
                    for (Map.Entry<String, HostAddress> entry : balanced.entrySet())
                    {
                        String host = entry.getValue().toString();
                        String path = entry.getKey();
                        locationDistribution.addCacheLocation(host, path);
                    }
                }
            } else
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
