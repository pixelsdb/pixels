package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.cache.CacheLocationDistribution;
import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import com.coreos.jetcd.data.KeyValue;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * pixels
 *
 * @author guodong
 */
public class TestCacheBalancer
{
    public static void main(String[] args)
    {
        TestCacheBalancer cacheBalancer = new TestCacheBalancer();
        try
        {
            CacheLocationDistribution locationDistribution = cacheBalancer.debug();
            System.out.println(locationDistribution.toString());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public CacheLocationDistribution debug() throws IOException
    {
        String pathUri = "hdfs://dbiir27:9000/pixels/pixels/test_1187/v_1_compact";
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create(pathUri), configuration);
        Path path = new Path(pathUri);
        // allocate: decide which node to cache each file
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        List<KeyValue> nodes = etcdUtil.getKeyValuesByPrefix(Constants.CACHE_NODE_STATUS_LITERAL);
        if (nodes == null || nodes.isEmpty()) {
            return null;
        }
        HostAddress[] hosts = new HostAddress[nodes.size()];
        FileStatus[] fileStatuses = fs.listStatus(path);
        int hostIndex = 0;
        for (int i = 0; i < nodes.size(); i++) {
            KeyValue node = nodes.get(i);
            // key: host_[hostname]; value: [status]. available if status == 1.
            hosts[hostIndex++] = HostAddress.fromString(node.getKey().toStringUtf8().substring(5));
        }

        CacheLocationDistribution locationDistribution = new CacheLocationDistribution(hosts, hostIndex);

        Map<String, Integer> nodesCacheStats = new HashMap<>();
        for (int i = 0; i < hostIndex; i++) {
            nodesCacheStats.put(hosts[i].toString(), 0);
        }

        for (FileStatus fileStatus : fileStatuses)
        {
            Set<HostAddress> locations = new HashSet<>();
            BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus.getPath(), 0, Long.MAX_VALUE);
            for (BlockLocation blockLocation : blockLocations)
            {
                locations.addAll(toHostAddress(blockLocation.getHosts()));
            }
            if (locations.size() == 0)
            {
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
                locationDistribution.addCacheLocation(chosenLocation.toString(), fileStatus.getPath().toString());
            }
        }

        return locationDistribution;
    }

    private List<HostAddress> toHostAddress(String[] hosts) {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts) {
            builder.add(HostAddress.fromString(host));
        }
        return builder.build();
    }
}
