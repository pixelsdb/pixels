package cn.edu.ruc.iir.pixels.daemon.cache;

import cn.edu.ruc.iir.pixels.cache.CacheLocationDistribution;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheConfig;
import cn.edu.ruc.iir.pixels.common.exception.FSException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CacheCoordinator is responsible for the following tasks:
 * 1. caching balance. It assigns each file a caching location, which are updated into etcd for global synchronization, and maintains a dynamic caching balance in the cluster.
 * 3. caching node monitor. It monitors all caching nodes(CacheManager) in the cluster, and update running status(available, busy, dead, etc.) of caching nodes.
 */
public class CacheCoordinator
        implements Server
{
    private static final Logger logger = LoggerFactory.getLogger(CacheCoordinator.class);
    private final EtcdUtil etcdUtil;
    private final PixelsCacheConfig cacheConfig;
    private final ScheduledExecutorService scheduledExecutor;
    private FSFactory fsFactory = null;
    // coordinator status: 0: init, 1: ready; -1: dead
    private AtomicInteger coordinatorStatus = new AtomicInteger(0);
    private CacheCoordinatorVersionRegister cacheVersionRegister = null;

    public CacheCoordinator()
    {
        this.etcdUtil = EtcdUtil.Instance();
        this.cacheConfig = new PixelsCacheConfig();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        initialize();
    }

    // initialize cache_version as 0 in etcd
    private void initialize()
    {
        // if cache has already been initialized, stop the initialization.
        if (null != etcdUtil.getKeyValue("cache_version")) {
            return;
        }
        Lease leaseClient = etcdUtil.getClient().getLeaseClient();
        try {
            long leaseId = leaseClient.grant(cacheConfig.getNodeLeaseTTL()).get(10, TimeUnit.SECONDS).getID();
            etcdUtil.putKeyValueWithLeaseId(Constants.CACHE_VERSION_LITERAL, "0", leaseId);
            this.cacheVersionRegister = new CacheCoordinatorVersionRegister(leaseClient, leaseId);
            scheduledExecutor.scheduleAtFixedRate(cacheVersionRegister, 1, 10, TimeUnit.SECONDS);
            coordinatorStatus.set(1);
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run()
    {
        Watch watch = etcdUtil.getClient().getWatchClient();
        Watch.Watcher watcher = watch.watch(
                ByteSequence.fromString(Constants.LAYOUT_VERSION_LITERAL), WatchOption.DEFAULT);
        // watch layout version change, and update cache distribution and cache version
        while (coordinatorStatus.get() > 0) {
            try {
                WatchResponse watchResponse = watcher.listen();
                for (WatchEvent event : watchResponse.getEvents()) {
                    if (event.getEventType() == WatchEvent.EventType.PUT) {
                        // update the cache distribution
                        int layoutVersion = Integer.parseInt(event.getKeyValue().getValue().toStringUtf8());
                        update(layoutVersion);
                        // update cache version
                        etcdUtil.putKeyValue(Constants.CACHE_VERSION_LITERAL, String.valueOf(layoutVersion));
                    }
                }
            }
            catch (InterruptedException | FSException e) {
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
        etcdUtil.delete(Constants.CACHE_VERSION_LITERAL);
        cacheVersionRegister.stop();
        this.scheduledExecutor.shutdownNow();
    }

    /**
     * Update file caching locations
     * 1. for all files, decide which files to cache
     * 2. for each file, decide which node to cache it
     * */
    private void update(int layoutVersion)
            throws FSException
    {
        if (fsFactory == null) {
//            fsFactory = FSFactory.Instance(cacheConfig.getHDFSConfigDir());
        }
        // select: decide which files to cache
        String[] paths = select();
        // allocate: decide which node to cache each file
        List<KeyValue> nodes = etcdUtil.getKeyValuesByPrefix(Constants.CACHE_NODE_STATUS_LITERAL);
        if (nodes.isEmpty()) {
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

    private String[] select()
    {
        return new String[]{"hdfs://dbiir01:9000//pixels/pixels/test_105/v_2_compact/0_201809232311590.compact.pxl"};
    }

    private void allocate(String[] paths, HostAddress[] nodes, int size, int layoutVersion)
            throws FSException
    {
        CacheLocationDistribution cacheLocationDistribution = assignCacheLocations(paths, nodes);
        for (int i = 0; i < size; i++)
        {
            HostAddress node = nodes[i];
            Set<String> files = cacheLocationDistribution.getCacheDistributionByLocation(node.toString());
            String key = "location_" + layoutVersion + "_" + node;
            etcdUtil.putKeyValue(key, String.join(",", files));
        }
    }

    private CacheLocationDistribution assignCacheLocations(String[] paths, HostAddress[] nodes)
            throws FSException
    {
        CacheLocationDistribution locationDistribution = new CacheLocationDistribution(nodes);

        Map<String, Integer> nodesCacheStats = new HashMap<>();
        for (HostAddress node : nodes) {
            nodesCacheStats.put(node.toString(), 0);
        }

        for (String path : paths)
        {
            // get a set of nodes where the file is located (location_set)
//            List<HostAddress> locations = fsFactory.getBlockLocations(new Path(path), 0, Long.MAX_VALUE);
            List<HostAddress> locations = Arrays.asList(nodes);
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

    private static class CacheCoordinatorVersionRegister
            implements Runnable
    {
        private final Lease leaseClient;
        private final long leaseId;

        CacheCoordinatorVersionRegister(Lease leaseClient, long leaseId)
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
