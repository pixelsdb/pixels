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
package io.pixelsdb.pixels.cache;

import com.facebook.presto.spi.HostAddress;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
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
