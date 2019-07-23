package io.pixelsdb.pixels.cache;

import com.facebook.presto.spi.HostAddress;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * pixels
 *
 * @author guodong
 */
public class CacheLocationDistribution
{
    private final Map<String, Set<String>> locationDistributionMap;

    /**
     * currently, cache location is allocated by the simple hash function.
     *
     * @param locations
     * @param size
     */
    public CacheLocationDistribution(HostAddress[] locations, int size)
    {
        this.locationDistributionMap = new HashMap<>(locations.length);
        for (int i = 0; i < size; i++)
        {
            locationDistributionMap.put(locations[i].toString(), new HashSet<>());
        }
    }

    public void addCacheLocation(String location, String file)
    {
        if (locationDistributionMap.get(location) != null)
        {
            locationDistributionMap.get(location).add(file);
        }
    }

    public Set<String> getCacheDistributionByLocation(String location)
    {
        return locationDistributionMap.get(location);
    }
}
