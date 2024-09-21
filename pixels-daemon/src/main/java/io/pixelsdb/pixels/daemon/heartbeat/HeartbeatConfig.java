/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.daemon.heartbeat;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 */
public class HeartbeatConfig
{
    private final ConfigFactory configFactory;

    public HeartbeatConfig()
    {
        this.configFactory = ConfigFactory.Instance();
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
}
