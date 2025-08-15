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

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author guodong, hank
 */
public class PixelsCacheConfig
{
    private final ConfigFactory configFactory;

    public PixelsCacheConfig()
    {
        this.configFactory = ConfigFactory.Instance();
    }

    public int getPartitions() { return Integer.parseInt(configFactory.getProperty("cache.partitions")); }

    public String getIndexLocation()
    {
        return configFactory.getProperty("index.base.location");
    }

    public long getIndexSize()
    {
        return Long.parseLong(configFactory.getProperty("index.size"));
    }

    public String getIndexDiskLocation()
    {
        return configFactory.getProperty("index.disk.location");
    }

    public String getCacheLocation()
    {
        return configFactory.getProperty("cache.base.location");
    }

    public long getCacheSize()
    {
        return Long.parseLong(configFactory.getProperty("cache.size"));
    }

    public int getZoneNum()
    {
        return Integer.parseInt(configFactory.getProperty("cache.zone.num"));
    }

    public int getSwapZoneNum()
    {
        return Integer.parseInt(configFactory.getProperty("cache.zone.swap.num"));
    }

    /**
     * Get the scheme of the underlying storage to be cached.
     * @return
     */
    public String getStorageScheme()
    {
         return configFactory.getProperty("cache.storage.scheme");
    }

    public String getHDFSConfigDir()
    {
        return configFactory.getProperty("hdfs.config.dir");
    }

    public int getNodeLeaseTTL()
    {
        int ttl = Integer.parseInt(configFactory.getProperty("heartbeat.lease.ttl.seconds"));
        int heartbeat = Integer.parseInt(configFactory.getProperty("heartbeat.period.seconds"));
        checkArgument(ttl > heartbeat);
        return ttl;
    }

    public int getNodeHeartbeatPeriod()
    {
        int heartbeat = Integer.parseInt(configFactory.getProperty("heartbeat.period.seconds"));
        checkArgument(heartbeat > 0);
        return heartbeat;
    }

    public boolean isCacheEnabled()
    {
        return Boolean.parseBoolean(configFactory.getProperty("cache.enabled"));
    }
}
