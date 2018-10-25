package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import com.coreos.jetcd.data.KeyValue;
import com.facebook.presto.spi.HostAddress;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PixelsCacheCoordinator is responsible for the following tasks:
 * 1. caching balance. It assigns each file a caching location, which are updated into etcd for global synchronization, and maintains a dynamic caching balance in the cluster.
 * 3. caching node monitor. It monitors all caching nodes(CacheManager) in the cluster, and update running status(available, busy, dead, etc.) of caching nodes.
 */
public class PixelsCacheCoordinator
{
    private final EtcdUtil etcdUtil;
    private final ConfigFactory config;
    private FSFactory fsFactory = null;

    private PixelsCacheCoordinator ()
    {
        this.etcdUtil = EtcdUtil.Instance();
        this.config = ConfigFactory.Instance();
        try {
            this.fsFactory = FSFactory.Instance(config.getProperty(""));
        }
        catch (FSException e) {
            e.printStackTrace();
        }
    }

    // initialize cache_version as 0 in etcd
    public boolean initialize()
    {
        if (fsFactory == null) {
            return false;
        }
        // if cache has already been initialized, stop the initialization.
        if (null != etcdUtil.getKeyValue("cache_version")) {
            return false;
        }
        etcdUtil.putKeyValue(Constants.CACHE_VERSION_LITERAL, "0");
        return true;
    }

    /**
     * Update file caching locations
     * */
    private void update(String[] paths, HostAddress[] nodes)
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
