package io.pixelsdb.pixels.common.balance;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableMap;

import java.util.*;

/**
 * Created at: 19-7-28
 * Author: hank
 */
public class ReplicaBalancer extends Balancer
{
    private boolean balanced = false;
    private Map<HostAddress, Integer> nodesCacheStats = new HashMap<>();
    private Map<String, Set<HostAddress>> origin = new HashMap<>();
    private Map<String, HostAddress> result = new HashMap<>();

    /**
     *
     * @param nodes the target node on which the files will be balanced to.
     */
    public ReplicaBalancer(List<HostAddress> nodes)
    {
        if (nodes == null)
        {
            return;
        }
        for (HostAddress node : nodes) {
            nodesCacheStats.put(node, 0);
        }
    }

    @Override
    public void put(String path, HostAddress address)
    {
        if (path == null || address == null)
        {
            return;
        }
        if (this.origin.containsKey(path))
        {
            this.origin.get(path).add(address);
        }
        else
        {
            Set<HostAddress> value = new HashSet<>(3);
            value.add(address);
            this.origin.put(path, value);
        }
        balanced = false;
    }

    @Override
    public void put(String path, Set<HostAddress> addresses)
    {
        if (path == null || addresses == null || addresses.isEmpty())
        {
            return;
        }
        for (HostAddress address : addresses)
        {
            // do not directly put address into origin,
            // for it may be modified from out scope.
            this.put(path, address);
        }
        balanced = false;
    }

    @Override
    public HostAddress get(String path)
    {
        return result.get(path);
    }

    @Override
    public Map<String, HostAddress> getAll()
    {
        ImmutableMap<String, HostAddress> all = ImmutableMap.copyOf(result);
        return all;
    }

    @Override
    public void balance()
    {
        if (balanced == false)
        {
            if (result.isEmpty() == false)
            {
                result.clear();
            }
            for (Map.Entry<String, Set<HostAddress>> entry : origin.entrySet())
            {
                String path = entry.getKey();
                Set<HostAddress> locations = entry.getValue();
                int leastCounter = Integer.MAX_VALUE;
                HostAddress chosenLocation = null;
                // find a node in the location_set with the least number of caching files
                for (HostAddress location : locations)
                {
                    if (nodesCacheStats.get(location) != null)
                    {
                        int count = nodesCacheStats.get(location);
                        if (count < leastCounter)
                        {
                            leastCounter = count;
                            chosenLocation = location;
                        }
                    }
                }

                if (chosenLocation != null) {
                    nodesCacheStats.put(chosenLocation, leastCounter+1);
                    result.put(path, chosenLocation);
                }
            }
            balanced = true;
        }
    }

    @Override
    public boolean isBalanced()
    {
        return balanced;
    }
}
