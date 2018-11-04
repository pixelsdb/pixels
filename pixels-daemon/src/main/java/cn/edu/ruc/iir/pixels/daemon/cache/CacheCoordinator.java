package cn.edu.ruc.iir.pixels.daemon.cache;

import cn.edu.ruc.iir.pixels.cache.CacheLocationDistribution;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheConfig;
import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import cn.edu.ruc.iir.pixels.daemon.Server;
import com.coreos.jetcd.data.KeyValue;
import com.facebook.presto.spi.HostAddress;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CacheCoordinator is responsible for the following tasks:
 * 1. caching balance. It assigns each file a caching location, which are updated into etcd for global synchronization, and maintains a dynamic caching balance in the cluster.
 * 3. caching node monitor. It monitors all caching nodes(CacheManager) in the cluster, and update running status(available, busy, dead, etc.) of caching nodes.
 */
public class CacheCoordinator
        implements Server
{
    private final EtcdUtil etcdUtil;
    private final PixelsCacheConfig cacheConfig;
    private FSFactory fsFactory = null;
    // coordinator status: 0: init, 1: ready; -1: dead
    private AtomicInteger coordinatorStatus = new AtomicInteger(0);

    private CacheCoordinator()
    {
        this.etcdUtil = EtcdUtil.Instance();
        this.cacheConfig = new PixelsCacheConfig();
        initialize();
    }

    // initialize cache_version as 0 in etcd
    private void initialize()
    {
        // if cache has already been initialized, stop the initialization.
        if (null != etcdUtil.getKeyValue("cache_version")) {
            return;
        }
        etcdUtil.putKeyValue(Constants.CACHE_VERSION_LITERAL, "0");
        coordinatorStatus.set(1);
    }

    @Override
    public void run()
    {
        // watch layout version change, and update cache distribution and cache version
        while (coordinatorStatus.get() > 0) {
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
    }

    /**
     * Update file caching locations
     * 1. for all files, decide which files to cache
     * 2. for each file, decide which node to cache it
     * */
    private void update()
            throws FSException
    {
        if (fsFactory == null) {
            fsFactory = FSFactory.Instance(cacheConfig.getHDFSConfigDir());
        }
        // select: decide which files to cache
        String[] paths = select();
        // allocate: decide which node to cache each file
        allocate(paths, new HostAddress[0]);
    }

    private String[] select()
    {
        return new String[0];
    }

    private void allocate(String[] paths, HostAddress[] nodes)
            throws FSException
    {
        CacheLocationDistribution cacheLocationDistribution = assignCacheLocations(paths, nodes);
        KeyValue cacheVersionKV = etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL);
        if (cacheVersionKV == null) {
            // todo deal with exception
            return;
        }
        String cacheVersion = cacheVersionKV.getValue().toStringUtf8();
        for (HostAddress node : nodes)
        {
            Set<String> files = cacheLocationDistribution.getCacheDistributionByLocation(node.toString());
            String key = "location_" + cacheVersion + "_" + node;
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
            List<HostAddress> locations = fsFactory.getBlockLocations(new Path(path), 0, Long.MAX_VALUE);
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
}
