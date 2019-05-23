package cn.edu.ruc.iir.pixels.daemon.cache;

import cn.edu.ruc.iir.pixels.cache.CacheLocationDistribution;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheConfig;
import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import cn.edu.ruc.iir.pixels.daemon.Server;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * CacheCoordinator is responsible for the following tasks:
 * 1. caching balance. It assigns each file a caching location, which are updated into etcd for global synchronization, and maintains a dynamic caching balance in the cluster.
 * 3. caching node monitor. It monitors all caching nodes(CacheManager) in the cluster, and update running status(available, busy, dead, etc.) of caching nodes.
 */
// todo cache location compaction. Cache locations for older versions still exist after being used.
public class CacheCoordinator
        implements Server
{
    private static final Logger logger = LogManager.getLogger(CacheCoordinator.class);
    private final EtcdUtil etcdUtil;
    private final PixelsCacheConfig cacheConfig;
    private final ScheduledExecutorService scheduledExecutor;
    private final MetadataService metadataService;
    private String hostName;
    private FileSystem fs = null;
    // coordinator status: 0: init, 1: ready; -1: dead
    private AtomicInteger coordinatorStatus = new AtomicInteger(0);
    private CacheCoordinatorRegister cacheCoordinatorRegister = null;
    private boolean initializeSuccess = false;

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

    // initialize cache_version as 0 in etcd
    private void initialize()
    {
        // if another cache coordinator exists, stop the initialization.
        if (null != etcdUtil.getKeyValue(Constants.CACHE_COORDINATOR_LITERAL)) {
            logger.warn("Another coordinator exists. Exit.");
            return;
        }
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            if (fs == null) {
                fs = FileSystem.get(URI.create(cacheConfig.getWarehousePath()), configuration);
            }
            // register coordinator
            Lease leaseClient = etcdUtil.getClient().getLeaseClient();
            long leaseId = leaseClient.grant(cacheConfig.getNodeLeaseTTL()).get(10, TimeUnit.SECONDS).getID();
            etcdUtil.putKeyValueWithLeaseId(Constants.CACHE_COORDINATOR_LITERAL, hostName, leaseId);
            this.cacheCoordinatorRegister = new CacheCoordinatorRegister(leaseClient, leaseId);
            scheduledExecutor.scheduleAtFixedRate(cacheCoordinatorRegister, 1, 10, TimeUnit.SECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
            // check version consistency
            int cache_version = 0, layout_version = 0;
            KeyValue cacheVersionKV = etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL);
            if (null != cacheVersionKV) {
                cache_version = Integer.parseInt(cacheVersionKV.getValue().toStringUtf8());
            }
            KeyValue layoutVersionKV = etcdUtil.getKeyValue(Constants.LAYOUT_VERSION_LITERAL);
            if (null != layoutVersionKV) {
                layout_version = Integer.parseInt(layoutVersionKV.getValue().toStringUtf8());
            }
            if (cache_version < layout_version) {
                logger.debug("Current cache version is left behind of current layout version. Update.");
                update(layout_version);
            }
            coordinatorStatus.set(CacheManager.CacheNodeStatus.READY.statusCode);
            initializeSuccess = true;
            logger.info("CacheCoordinator on " + hostName + " has started.");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run()
    {
        logger.info("Starting cache coordinator");
        if (false == initializeSuccess) {
            logger.info("Initialization failed, stop now.");
            return;
        }
        Watch watch = etcdUtil.getClient().getWatchClient();
        Watch.Watcher watcher = watch.watch(
                ByteSequence.fromString(Constants.LAYOUT_VERSION_LITERAL), WatchOption.DEFAULT);
        // watch layout version change, and update cache distribution and cache version
        while (coordinatorStatus.get() >= 0) {
            try {
                // layout version can be changed by rainbow.
                WatchResponse watchResponse = watcher.listen();
                for (WatchEvent event : watchResponse.getEvents()) {
                    if (event.getEventType() == WatchEvent.EventType.PUT) {
                        logger.debug("Update cache distribution");
                        // update the cache distribution
                        int layoutVersion = Integer.parseInt(event.getKeyValue().getValue().toStringUtf8());
                        update(layoutVersion);
                        // update cache version, notify cache managers on each node to update cache.
                        logger.debug("Update cache version to " + layoutVersion);
                        etcdUtil.putKeyValue(Constants.CACHE_VERSION_LITERAL, String.valueOf(layoutVersion));
                    }
                }
            }
            catch (InterruptedException | MetadataException | IOException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
                break;
            }
        }
    }

    @Override
    public boolean isRunning()
    {
        return coordinatorStatus.get() >= 0;
    }

    @Override
    public void shutdown()
    {
        // TODO: check if the objects here are not null in case that coordinator was not started successfully.
        coordinatorStatus.set(CacheManager.CacheNodeStatus.UNHEALTHY.statusCode);
        cacheCoordinatorRegister.stop();
        etcdUtil.delete(Constants.CACHE_COORDINATOR_LITERAL);
        logger.info("CacheCoordinator shuts down.");
        this.scheduledExecutor.shutdownNow();
    }

    /**
     * Update file caching locations
     * 1. for all files, decide which files to cache
     * 2. for each file, decide which node to cache it
     * */
    private void update(int layoutVersion)
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
            if (Integer.parseInt(node.getValue().toStringUtf8()) == CacheManager.CacheNodeStatus.READY.statusCode) {
                hosts[hostIndex++] = HostAddress.fromString(node.getKey().toStringUtf8().substring(5));
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
        List<Path> files = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(new Path(compactPath));
        if (fileStatuses != null) {
            for (FileStatus fileStatus : fileStatuses)
            {
                if (fileStatus.isFile()) {
                    files.add(fileStatus.getPath());
                }
            }
        }
        String[] result = new String[files.size()];
        List<String> paths = files.stream().map(Path::toString).collect(Collectors.toList());
        paths.toArray(result);
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

        Map<String, Integer> nodesCacheStats = new HashMap<>();
        for (int i = 0; i < size; i++) {
            nodesCacheStats.put(nodes[i].toString(), 0);
        }

        for (String path : paths)
        {
            // get a set of nodes where the blocks of the file is located (location_set)
            Set<HostAddress> locations = new HashSet<>();
            BlockLocation[] blockLocations = fs.getFileBlockLocations(new Path(path), 0, Long.MAX_VALUE);
            for (BlockLocation blockLocation : blockLocations)
            {
                locations.addAll(toHostAddress(blockLocation.getHosts()));
            }
            if (locations.size() == 0) {
                continue;
            }
            int leastCounter = Integer.MAX_VALUE;
            HostAddress chosenLocation = null;
            // find a node in the location_set with the least number of caching files
            for (HostAddress location : locations)
            {
                if (nodesCacheStats.get(location.toString()) != null) {
                    int count = nodesCacheStats.get(location.toString());
                    if (count < leastCounter) {
                        leastCounter = count;
                        chosenLocation = location;
                    }
                }
            }
            if (chosenLocation != null) {
                nodesCacheStats.put(chosenLocation.toString(), leastCounter+1);
                locationDistribution.addCacheLocation(chosenLocation.toString(), path);
            }
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
