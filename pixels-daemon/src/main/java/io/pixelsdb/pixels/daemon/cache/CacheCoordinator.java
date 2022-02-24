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

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
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
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CacheCoordinator is responsible for the following tasks:
 * 1. caching balance. It assigns each file a caching location, which are updated into etcd for global synchronization, and maintains a dynamic caching balance in the cluster.
 * 3. caching node monitor. It monitors all caching nodes(CacheManager) in the cluster, and update running status(available, busy, dead, etc.) of caching nodes.
 *
 * @author guodong
 * @author hank
 */
// todo cache location compaction. Cache locations for older versions still exist after being used.
public class CacheCoordinator
        implements Server
{
    enum CoordinatorStatus
    {
        DEAD(-1), INIT(0), READY(1);

        public final int StatusCode;
        CoordinatorStatus(int statusCode)
        {
            this.StatusCode = statusCode;
        }
    }

    private static final Logger logger = LogManager.getLogger(CacheCoordinator.class);
    private final EtcdUtil etcdUtil;
    private final PixelsCacheConfig cacheConfig;
    private final ScheduledExecutorService scheduledExecutor;
    private final MetadataService metadataService;
    private String hostName;
    private Storage storage = null;
    // coordinator status: 0: init, 1: ready; -1: dead
    private AtomicInteger coordinatorStatus = new AtomicInteger(CoordinatorStatus.INIT.StatusCode);
    private CacheCoordinatorRegister cacheCoordinatorRegister = null;
    private boolean initializeSuccess = false;
    private CountDownLatch runningLatch;

    public CacheCoordinator()
    {
        this.etcdUtil = EtcdUtil.Instance();
        this.cacheConfig = new PixelsCacheConfig();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.metadataService = new MetadataService(cacheConfig.getMetaHost(), cacheConfig.getMetaPort());
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
        initialize();
    }

    /**
     * Initialize Coordinator:
     *
     * 1. check if there is an existing coordinator, if yes, return.
     * 2. register the coordinator.
     * 3. create the storage instance that is used to get the metadata of the storage.
     * 4. check the local and the global cache versions, update the cache plan if needed.
     */
    private void initialize()
    {
        // 1. if another cache coordinator exists, stop the initialization.
        KeyValue coordinatorKV = etcdUtil.getKeyValue(Constants.CACHE_COORDINATOR_LITERAL);
        if (coordinatorKV != null && coordinatorKV.getLease() > 0)
        {
            /**
             * Issue #181:
             * When the lease of the coordinator key exists, wait for the lease ttl to expire.
             */
            try
            {
                Thread.sleep(cacheConfig.getNodeLeaseTTL() * 1000);
            } catch (InterruptedException e)
            {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
            coordinatorKV = etcdUtil.getKeyValue(Constants.CACHE_COORDINATOR_LITERAL);
            if (coordinatorKV != null && coordinatorKV.getLease() > 0)
            {
                // another coordinator exists
                logger.error("Another coordinator exists, exit...");
                return;
            }
        }
        try {
            // 2. register the coordinator
            Lease leaseClient = etcdUtil.getClient().getLeaseClient();
            long leaseId = leaseClient.grant(cacheConfig.getNodeLeaseTTL()).get(10, TimeUnit.SECONDS).getID();
            etcdUtil.putKeyValueWithLeaseId(Constants.CACHE_COORDINATOR_LITERAL, hostName, leaseId);
            this.cacheCoordinatorRegister = new CacheCoordinatorRegister(leaseClient, leaseId);
            scheduledExecutor.scheduleAtFixedRate(cacheCoordinatorRegister,
                    0, cacheConfig.getNodeHeartbeatPeriod(), TimeUnit.SECONDS);
            // 3. create the storage instance.
            if (storage == null) {
                storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());
            }
            // 4. check version consistency
            int cache_version = 0, layout_version = 0;
            KeyValue cacheVersionKV = etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL);
            if (null != cacheVersionKV) {
                cache_version = Integer.parseInt(cacheVersionKV.getValue().toString(StandardCharsets.UTF_8));
            }
            KeyValue layoutVersionKV = etcdUtil.getKeyValue(Constants.LAYOUT_VERSION_LITERAL);
            if (null != layoutVersionKV) {
                layout_version = Integer.parseInt(layoutVersionKV.getValue().toString(StandardCharsets.UTF_8));
            }
            if (cache_version < layout_version) {
                logger.debug("Current cache version is left behind of current layout version, update the cache...");
                updateCachePlan(layout_version);
            }
            coordinatorStatus.set(CoordinatorStatus.READY.StatusCode);
            initializeSuccess = true;
            logger.info("CacheCoordinator on " + hostName + " has started.");
        }
        catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void run()
    {
        logger.info("Starting Coordinator");
        if (false == initializeSuccess) {
            logger.error("Initialization failed, stop now...");
            return;
        }
        runningLatch = new CountDownLatch(1);
        // watch layout version change, and update cache distribution and cache version
        Watch.Watcher watcher = etcdUtil.getClient().getWatchClient().watch(
                ByteSequence.from(Constants.LAYOUT_VERSION_LITERAL, StandardCharsets.UTF_8),
                WatchOption.DEFAULT, watchResponse ->
        {
            for (WatchEvent event : watchResponse.getEvents())
            {
                if (event.getEventType() == WatchEvent.EventType.PUT)
                {
                    // listen to the PUT even on the LAYOUT VERSION, which can be changed by rainbow.
                    if (coordinatorStatus.get() == CoordinatorStatus.READY.StatusCode)
                    {
                        try
                        {
                            // this coordinator is ready.
                            logger.debug("Update cache distribution");
                            // update the cache distribution
                            int layoutVersion = Integer.parseInt(event.getKeyValue().getValue().toString(StandardCharsets.UTF_8));
                            updateCachePlan(layoutVersion);
                            // update cache version, notify cache managers on each node to update cache.
                            logger.debug("Update cache version to " + layoutVersion);
                            etcdUtil.putKeyValue(Constants.CACHE_VERSION_LITERAL, String.valueOf(layoutVersion));
                        } catch (IOException | MetadataException e)
                        {
                            logger.error(e.getMessage());
                            e.printStackTrace();
                        }
                    } else if (coordinatorStatus.get() == CoordinatorStatus.DEAD.StatusCode)
                    {
                        // coordinator is shutdown.
                        runningLatch.countDown();
                    }
                    break;
                }
            }
        });
        try
        {
            // Wait for this coordinator to be shutdown.
            runningLatch.await();
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
        int status = coordinatorStatus.get();
        return status == CoordinatorStatus.INIT.StatusCode ||
                status == CoordinatorStatus.READY.StatusCode;
    }

    @Override
    public void shutdown()
    {
        coordinatorStatus.set(CoordinatorStatus.DEAD.StatusCode);
        logger.debug("Shutting down Coordinator...");
        scheduledExecutor.shutdownNow();
        if (cacheCoordinatorRegister != null)
        {
            cacheCoordinatorRegister.stop();
        }
        etcdUtil.delete(Constants.CACHE_COORDINATOR_LITERAL);
        if (metadataService != null)
        {
            try
            {
                metadataService.shutdown();
            } catch (InterruptedException e)
            {
                logger.error("Failed to shutdown rpc channel for metadata service " +
                        "while shutting down Coordinator.", e);
                e.printStackTrace();
            }
        }
        if (storage != null)
        {
            try
            {
                storage.close();
            } catch (IOException e)
            {
                logger.error("Failed to close cache storage while shutting down Coordinator.", e);
                e.printStackTrace();
            }
        }
        if (runningLatch != null)
        {
            runningLatch.countDown();
        }
        EtcdUtil.Instance().getClient().close();
        logger.info("Coordinator on '" + hostName + "' is shutdown.");
    }

    /**
     * Update file caching locations:
     * 1. for all files, decide which files to cache
     * 2. for each file, decide which node to cache it
     * */
    private void updateCachePlan(int layoutVersion)
            throws MetadataException, IOException
    {
        Layout layout = metadataService.getLayout(cacheConfig.getSchema(), cacheConfig.getTable(), layoutVersion);
        // select: decide which files to cache
        assert layout != null;
        String[] paths = select(layout);
        // allocate: decide which node to cache each file
        List<KeyValue> nodes = etcdUtil.getKeyValuesByPrefix(Constants.CACHE_NODE_STATUS_LITERAL);
        if (nodes == null || nodes.isEmpty()) {
            logger.info("Nodes is null or empty, no updates");
            return;
        }
        HostAddress[] hosts = new HostAddress[nodes.size()];
        int hostIndex = 0;
        for (int i = 0; i < nodes.size(); i++) {
            KeyValue node = nodes.get(i);
            // key: host_[hostname]; value: [status]. available if status == 1.
            if (Integer.parseInt(node.getValue().toString(StandardCharsets.UTF_8)) == CacheManager.CacheNodeStatus.READY.StatusCode) {
                hosts[hostIndex++] = HostAddress.fromString(node.getKey().toString(StandardCharsets.UTF_8).substring(5));
            }
        }
        allocate(paths, hosts, hostIndex, layoutVersion);
    }

    /**
     * get the HDFS file paths under the compact path of the first layout.
     * @param layout
     * @return the file paths
     * @throws IOException
     */
    private String[] select(Layout layout)
            throws IOException
    {
        String compactPath = layout.getCompactPath();
        List<String> files = new ArrayList<>();
        List<Status> statuses = storage.listStatus(compactPath);
        if (statuses != null) {
            for (Status status : statuses)
            {
                if (status.isFile()) {
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
    private void allocate(String[] paths, HostAddress[] nodes, int size, int layoutVersion)
            throws IOException
    {
        CacheLocationDistribution cacheLocationDistribution = assignCacheLocations(paths, nodes, size);
        for (int i = 0; i < size; i++)
        {
            HostAddress node = nodes[i];
            Set<String> files = cacheLocationDistribution.getCacheDistributionByLocation(node.toString());
            String key = Constants.CACHE_LOCATION_LITERAL + layoutVersion + "_" + node;
            logger.debug(files.size() + " files are allocated to " + node + " at version" + layoutVersion);
            etcdUtil.putKeyValue(key, String.join(";", files));
        }
    }

    /**
     * assign hdfs files to cache manager nodes randomly, guaranty load balance.
     * @param paths
     * @param nodes
     * @param size
     * @return
     * @throws IOException
     */
    private CacheLocationDistribution assignCacheLocations(String[] paths, HostAddress[] nodes, int size)
            throws IOException
    {
        CacheLocationDistribution locationDistribution = new CacheLocationDistribution(nodes, size);

        List<HostAddress> cacheNodes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            cacheNodes.add(nodes[i]);
        }

        Balancer replicaBalancer = new ReplicaBalancer(cacheNodes);
        for (String path : paths)
        {
            // get a set of nodes where the blocks of the file is located (location_set)
            Set<HostAddress> addresses = new HashSet<>();
            List<Location> locations = storage.getLocations(path);
            for (Location location : locations)
            {
                addresses.addAll(toHostAddress(location.getHosts()));
            }
            if (addresses.size() == 0)
            {
                continue;
            }
            replicaBalancer.put(path, addresses);
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
                        throw new BalancerException("absolute balancer failed to balance paths.");
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
                throw new BalancerException("replica balancer failed to balance paths.");
            }
        } catch (BalancerException e)
        {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        return locationDistribution;
    }

    private List<HostAddress> toHostAddress(String[] hosts) {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts) {
            builder.add(HostAddress.fromString(host));
//            break;
        }
        return builder.build();
    }

    /**
     * TODO: there may be a gap between two calls of keepAliveOnce.
     */
    private static class CacheCoordinatorRegister
            implements Runnable
    {
        private final Lease leaseClient;
        private final long leaseId;

        CacheCoordinatorRegister(Lease leaseClient, long leaseId)
        {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        @Override
        public void run()
        {
            leaseClient.keepAliveOnce(leaseId);
        }

        public void stop()
        {
            leaseClient.revoke(leaseId);
            leaseClient.close();
        }
    }
}
