package cn.edu.ruc.iir.pixels.daemon.cache;

import cn.edu.ruc.iir.pixels.cache.CacheLocationDistribution;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheConfig;
import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
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
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final String hostName;
    private FSFactory fsFactory = null;
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
                logger.info("Current cache version is left behind of current layout version. Update.");
                update(layout_version);
            }
            coordinatorStatus.set(1);
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
        while (coordinatorStatus.get() > 0) {
            try {
                WatchResponse watchResponse = watcher.listen();
                for (WatchEvent event : watchResponse.getEvents()) {
                    if (event.getEventType() == WatchEvent.EventType.PUT) {
                        logger.info("Update cache distribution");
                        // update the cache distribution
                        int layoutVersion = Integer.parseInt(event.getKeyValue().getValue().toStringUtf8());
                        update(layoutVersion);
                        // update cache version
                        logger.info("Update cache version to " + layoutVersion);
                        etcdUtil.putKeyValue(Constants.CACHE_VERSION_LITERAL, String.valueOf(layoutVersion));
                    }
                }
            }
            catch (InterruptedException | FSException | MetadataException e) {
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
        coordinatorStatus.set(-1);
        cacheCoordinatorRegister.stop();
        etcdUtil.delete(Constants.CACHE_COORDINATOR_LITERAL);
        this.scheduledExecutor.shutdownNow();
    }

    /**
     * Update file caching locations
     * 1. for all files, decide which files to cache
     * 2. for each file, decide which node to cache it
     * */
    private void update(int layoutVersion)
            throws FSException, MetadataException
    {
        if (fsFactory == null) {
            fsFactory = FSFactory.Instance(cacheConfig.getHDFSConfigDir());
        }
        List<Layout> layout = metadataService.getLayout(cacheConfig.getSchema(), cacheConfig.getTable(), layoutVersion);
        // select: decide which files to cache
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
            if (Integer.parseInt(node.getValue().toStringUtf8()) == 1) {
                hosts[hostIndex++] = HostAddress.fromString(node.getKey().toStringUtf8().substring(5));
            }
        }
        allocate(paths, hosts, hostIndex, layoutVersion);
    }

    private String[] select(List<Layout> layout)
            throws FSException
    {
        if (layout.isEmpty()) {
            logger.info("Layout is empty, return a 0-length array");
            return new String[0];
        }
        else {
            String compactPath = layout.get(0).getCompactPath();
            List<Path> files = fsFactory.listFiles(compactPath);
            String[] result = new String[files.size()];
            List<String> paths = files.stream().map(Path::toString).collect(Collectors.toList());
            paths.toArray(result);
            return result;
        }
    }

    private void allocate(String[] paths, HostAddress[] nodes, int size, int layoutVersion)
            throws FSException
    {
        CacheLocationDistribution cacheLocationDistribution = assignCacheLocations(paths, nodes, size);
        for (int i = 0; i < size; i++)
        {
            HostAddress node = nodes[i];
            Set<String> files = cacheLocationDistribution.getCacheDistributionByLocation(node.toString());
            String key = Constants.CACHE_LOCATION_LITERAL + layoutVersion + "_" + node;
            etcdUtil.putKeyValue(key, String.join(";", files));
        }
    }

    private CacheLocationDistribution assignCacheLocations(String[] paths, HostAddress[] nodes, int size)
            throws FSException
    {
        CacheLocationDistribution locationDistribution = new CacheLocationDistribution(nodes, size);

        Map<String, Integer> nodesCacheStats = new HashMap<>();
        for (int i = 0; i < size; i++) {
            nodesCacheStats.put(nodes[i].toString(), 0);
        }

        for (String path : paths)
        {
            // get a set of nodes where the file is located (location_set)
            List<HostAddress> locations = fsFactory.getBlockLocations(new Path(path), 0, Long.MAX_VALUE);
//            List<HostAddress> locations = Arrays.asList(nodes);
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
                locationDistribution.addCacheLocation(chosenLocation.toString(), path);
            }
        }

        return locationDistribution;
    }

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
